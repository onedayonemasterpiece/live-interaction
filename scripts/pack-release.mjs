// Release distribution only: refuses uncommitted source, prints no credentials.
import {execFileSync} from 'node:child_process';
import {readFileSync,writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {resolve,join} from 'node:path';
const destination=process.argv[2];if(!destination)throw Error('Usage: node scripts/pack-release.mjs <managed artifact directory>');
if(execFileSync('git',['status','--porcelain'],{encoding:'utf8'}).trim())throw Error('Commit all framework changes before packaging');
const commit=execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim();
const packed=JSON.parse(execFileSync('npm',['pack','--json','--pack-destination',resolve(destination)],{encoding:'utf8'}))[0];
const archive=join(resolve(destination),packed.filename),sha256=createHash('sha256').update(readFileSync(archive)).digest('hex');
const version=JSON.parse(readFileSync('package.json','utf8')).version;
const receipt={version,repository:'https://github.com/onedayonemasterpiece/live-interaction',commit,archive,sha256};
writeFileSync(join(resolve(destination),'release.json'),JSON.stringify(receipt,null,2)+'\n');console.log(JSON.stringify(receipt));
