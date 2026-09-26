import {copyFileSync,mkdirSync,readdirSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {join} from 'node:path';
export function installBrowserAssets(destination){
  const source=fileURLToPath(new URL('../browser/',import.meta.url));mkdirSync(destination,{recursive:true});
  const names=readdirSync(source).filter(n=>n.endsWith('.js')).sort();
  for(const name of names)copyFileSync(join(source,name),join(destination,name));
  return names;
}
