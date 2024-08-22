# DAQ Scripts

## translate.py

This script takes the following arguments:

- **--src**: path to the data file that will be translated. 
By default, the parser will first check `.../data/{src}` (in case the `src` has no suffix, the parser will look for `{src}.txt` instead). If this path doesn't exist, it will check `{src}`.

    > For example `--src file` will parse to `.../data/file.txt` (or `file.txt` if the previous path doesn't exist) and `--src .../dir/file` will parse to `.../dir/file.txt`; whereas `--src file.dat` will parse to `.../data/file.dat` (or `file.dat`).

- **--lang, -l**: language from which to translate from the following:
    - `it`

    > The only implemented language is `it` for `Italian`, but more user-defined languages could be implemented through the `Language` base class. Each language is defined by the way the bytes are unpacked (mask and position in the word) and the type of the value it will be cast to (int8, int16, etc).

- **--out**: path to store the new file that will have a tabular format (`.parquet`). As in the case of `--src`, it will first check the default data folder `.../data`, but in this case it will overwrite the file suffix to `.parquet`.

- **--cfg**: path to the configuration file that will be used in the translation. This file must have a given format, similar to the `.yaml` files that may be found under `.../cfg/daq/mapping`. By default, the parser will first check `.../cfg/daq/mapping/{cfg}.yaml` if it doesn't exist, it will check `{cfg}`.

- **--buffer-size**: maximum size the buffer will grow to (in lines) until it is dumped to the output `.parquet` file. Since I/O is computationally costly, setting a low number will increase the run time; setting a high number, will increase the memory consumption. By default, this value is `100000` (100k), which is a compromise between memory usage and computing time.

- **--debug**: turns on the debugging mode. This requires the package `alive_progress` to run and takes a while since it needs to check the number of lines in the source file. However, this adds the utility features such as a progress bar and ETA, which are most useful. By default, this is turned off.


### Example
```bash
<<<<<<< HEAD
<<<<<<< HEAD
python translate.py -l it\
    --src /afs/cern.ch/user/r/redondo/work/public/sxa5/testpulse_theta_2.txt\
    --out test\
=======
python translate.py /afs/cern.ch/user/r/redondo/work/public/sxa5/testpulse_theta_2.txt test\
    -l it\
>>>>>>> First local commit
=======
python translate.py /afs/cern.ch/user/r/redondo/work/public/sxa5/testpulse_theta_2.txt test\
    -l it\
>>>>>>> cd7ff81b9f591768977cc27777949b747be49a37
    --cfg testpulse_theta_2
```
This example takes the data taken with the slow control box test shared by Ignacio in `/afs/cern.ch/user/r/redondo/work/public/sxa5/testpulse_theta_2.txt` with an OBDT configured in the file `.../cfg/mapping/testpulse_theta_2.yaml` and translates from `it` (Italian) and stores the tabular dataset in `.../data/test.parquet`.

> ### Tip: Partial translation!
> The program may be exited at any point of the loop raising a `KeyboardInterrupt` excpection (i.e., pressing `Ctrl+C`). This will be handled properly and the file will be saved up to the point of the interruption. This is useful if one wants to produce a partial translation on a reduced portion of the data.

