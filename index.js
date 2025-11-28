const core = require('@actions/core');
const github = require('@actions/github');
const COS = require('cos-nodejs-sdk-v5');
const fs = require('fs');
const Path = require('path');

const walk = async (path, walkFn) => {
    stats = await fs.promises.lstat(path);
    if (!stats.isDirectory()) {
        return await walkFn(path);
    }

    const dir = await fs.promises.opendir(path);
    for await (const dirent of dir) {
        await walk(Path.join(path, dirent.name), walkFn);
    }
}

const uploadFileToCOS = (cos, path) => {
    return new Promise((resolve, reject) => {
        cos.cli.putObject({
            Bucket: cos.bucket,
            Region: cos.region,
            Key: Path.join(cos.remotePath, path),
            StorageClass: 'STANDARD',
            Body: fs.createReadStream(Path.join(cos.localPath, path)),
        }, function(err, data) {
            if (err) {
                return reject(err);
            } else {
                return resolve(data);
            }
        });
    });
}

const deleteFileFromCOS = (cos, path) => {
    return new Promise((resolve, reject) => {
        cos.cli.deleteObject({
            Bucket: cos.bucket,
            Region: cos.region,
            Key: Path.join(cos.remotePath, path)
        }, function(err, data) {
            if (err) {
                return reject(err);
            } else {
                return resolve(data);
            }
        });
    });
}

const listFilesOnCOS = (cos, nextMarker) => {
    return new Promise((resolve, reject) => {
        cos.cli.getBucket({
            Bucket: cos.bucket,
            Region: cos.region,
            Prefix: cos.remotePath,
            NextMarker: nextMarker
        }, function(err, data) {
            if (err) {
                return reject(err);
            } else {
                return resolve(data);
            }
        });
    });
}

const collectLocalFiles = async (cos) => {
    const root = cos.localPath;
    const files = new Set();
    await walk(root, (path) => {
        let p = path.substring(root.length);
        for (;p[0] === '/';) {
            p = p.substring(1);
        }
        files.add(p);
    });
    return files;
}

const uploadFiles = async (cos, localFiles) => {
    const size = localFiles.size;
    let index = 0;
    let percent = 0;
    for (const file of localFiles) {
        await uploadFileToCOS(cos, file);
        index++;
        percent = parseInt(index / size * 100);
        console.log(`>> [${index}/${size}, ${percent}%] uploaded ${Path.join(cos.localPath, file)}`);
    }
}

const collectRemoteFiles = async (cos) => {
    const files = new Set();
    let data = {};
    let nextMarker = null;

    do {
        data = await listFilesOnCOS(cos, nextMarker);
        for (const e of data.Contents) {
            let p = e.Key.substring(cos.remotePath.length);
            for (;p[0] === '/';) {
                p = p.substring(1);
            }
            files.add(p);
        }
        nextMarker = data.NextMarker;
    } while (data.IsTruncated === 'true');

    return files;
}


const findDeletedFiles = (localFiles, remoteFiles) => {
    const deletedFiles = new Set();
    for (const file of remoteFiles) {
        if (!localFiles.has(file)) {
            deletedFiles.add(file);
        }
    }
    return deletedFiles;
}

const cleanDeleteFiles = async (cos, deleteFiles) => {
    const size = deleteFiles.size;
    let index = 0;
    let percent = 0;
    for (const file of deleteFiles) {
        await deleteFileFromCOS(cos, file);
        index++;
        percent = parseInt(index / size * 100);
        console.log(`>> [${index}/${size}, ${percent}%] cleaned ${Path.join(cos.remotePath, file)}`);
    }
}

const process = async (cos) => {
    const localFiles = await collectLocalFiles(cos);
    console.log(localFiles.size, 'files to be uploaded');
    await uploadFiles(cos, localFiles);
    let cleanedFilesCount = 0;
    if (cos.clean) {
        const remoteFiles = await collectRemoteFiles(cos);
        const deletedFiles = findDeletedFiles(localFiles, remoteFiles);
        if (deletedFiles.size > 0) {
            console.log(`${deletedFiles.size} files to be cleaned`);
        }
        await cleanDeleteFiles(cos, deletedFiles);
        cleanedFilesCount = deletedFiles.size;
    }
    let cleanedFilesMessage = '';
    if (cleanedFilesCount > 0) {
        cleanedFilesMessage = `, cleaned ${cleanedFilesCount} files`;
    }
    console.log(`uploaded ${localFiles.size} files${cleanedFilesMessage}`);
}

try {
    const cos = {
        cli: new COS({
            SecretId: core.getInput('secret_id'),
            SecretKey: core.getInput('secret_key'),
            Domain: core.getInput('accelerate') === 'true' ? '{Bucket}.cos.accelerate.myqcloud.com' : undefined,
        }),
        bucket: core.getInput('cos_bucket'),
        region: core.getInput('cos_region'),
        localPath: core.getInput('local_path'),
        remotePath: core.getInput('remote_path'),
        clean: core.getInput('clean') === 'true'
    };

    process(cos).catch((reason) => {
        core.setFailed(`fail to upload files to cos: ${JSON.stringify(reason, Object.getOwnPropertyNames(reason))}`);
    });
} catch (error) {
    core.setFailed(error.message);
}
