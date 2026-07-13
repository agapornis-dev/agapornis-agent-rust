const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

const required = name => {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
};
const directory = required('RELEASE_DIRECTORY');
const repository = required('GITHUB_REPOSITORY');
const tag = required('GITHUB_REF_NAME');
const artifact = file => {
  const target = path.join(directory, file);
  const body = fs.readFileSync(target);
  return {
    url: `https://github.com/${repository}/releases/download/${tag}/${file}`,
    sha256: crypto.createHash('sha256').update(body).digest('hex'),
    sizeBytes: body.length,
  };
};
const manifest = {
  schemaVersion: 1,
  component: 'agent',
  version: tag.replace(/^v/, ''),
  channel: 'stable',
  publishedAt: new Date().toISOString(),
  releaseUrl: `https://github.com/${repository}/releases/tag/${tag}`,
  artifacts: {
    'linux-x86_64': artifact('agapornis-agent-linux-x86_64'),
    'linux-aarch64': artifact('agapornis-agent-linux-aarch64'),
  },
};
fs.writeFileSync(path.join(directory, 'release-manifest.json'), `${JSON.stringify(manifest, null, 2)}\n`);
