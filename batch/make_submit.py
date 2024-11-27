import sys
import argparse
from pathlib import Path
import shutil
import yaml
import subprocess

here   = Path(__file__).resolve().parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--cpus',  default=1,    help='number of CPU cores to request.')
parser.add_argument('--alias', default=None, help='alias of job to store.')
parser.add_argument('--conda', default=None, help='path to the environment of conda. By default, will take the one stored in batch/config.yaml.')
parser.add_argument('-f',      dest = 'force',action='store_true', help='whether to force the submission of the job to batch.')
parser.add_argument('--jobs',  dest = 'jobs_path', default=here / 'jobs', help='path to the directory where to store the job information.')
parser.add_argument('--logs',  dest = 'logs_path', default=here / 'logs', help='path to the directory where to store the log information.')

with open(here / 'config.yaml') as f:
    config = yaml.safe_load(f)
    if parser.parse_known_args()[0].conda is None:
        parser.set_defaults(conda = config['conda'])


args, script_args = parser.parse_known_args()

try:
    script = Path(script_args[0]).resolve()
except RuntimeError:
    script = Path(script_args[0]).readlink().resolve()
script_args   = script_args[1:]

if args.alias is None:
    alias = script.stem
else:
    alias = args.alias

condor_sh = (
    f"export PATH={here.parent/'src'}:$PATH\n"
    f"source /etc/profile.d/conda.sh\n"
    f"conda activate {args.conda}\n"
    f"python3 {script} {' '.join(script_args)}"
)
print('Batch will run these commands:\n',condor_sh,'\n')

with open(here / 'condorExecutable.sh', 'w+') as f:
    f.write(condor_sh)

exe_path  = Path(f'{args.jobs_path}/{alias}/condorExecutable.sh')
exe_path.parent.mkdir(parents = True, exist_ok = True)
with open(exe_path, 'w+') as f:
    f.write(condor_sh)


log_path = Path(args.logs_path) / alias
if log_path.exists(): shutil.rmtree(log_path)
log_path.mkdir(parents = True, exist_ok = True)

condor_sub = f"""\
executable             = {here}/condorExecutable.sh
+CieIncludeAF          = True
request_cpus           = {args.cpus}
#
output                 = {log_path}/job.out
log                    = {log_path}/job.log
error                  = {log_path}/job.err
#
queue 1
"""

print("This job will be submitted to batch:\n",condor_sub, '\n')
with open(here / 'condorSubmit.sub', 'w+') as f:
    f.write(condor_sub)
    
sub_path = exe_path.with_suffix('.sub')
sub_path.parent.mkdir(parents = True, exist_ok = True)
with open(sub_path, 'w+') as f:
    f.write(condor_sub)

# Submitting the job
if args.force:
    subprocess.run(['condor_submit','-remote','condorsc1.ciemat.es', str(sub_path)])