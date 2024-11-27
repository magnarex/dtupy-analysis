# DAQ Scripts

## translate.py

This script takes the following arguments:

- **src_path**: path to the data file that will be translated. 
By default, the parser will first check `data/{src}` (in case the `src` has no suffix, the parser will look for `{src}.txt` instead). If this path doesn't exist, it will check `{src}`.
    > For example `file` will parse to `data/file.txt` (or `file.txt` if the previous path doesn't exist) and `.../dir/file` will parse to `.../dir/file.txt`; whereas `file.dat` will parse to `.../data/file.dat` (or `file.dat` if it exists).
    
    - For a `dtupy` dump, the type of files to be read need to be given as follows:
        - **hit files**: `dump/hit`. This will search for all files that match `dump/hit*.bin` pattern and will translate them into the same output file.


- **out_path**: path to the file where to store the read data. This will be a `.parquet` independently of the specified extension and will follow the same parsing rules as above.

- **-l**: language from which to translate from the following:
    - `it`: new fromat from the Padova's Slow Control box (includes orbit counter).
    - `la`: old format from the Padova's Slow Control Box (doesn't include orbit counter).
    - `es`: binaries produced with `dtupy`.

    > These are the default defined languages, but more user-defined languages could be implemented through the `Language` base class and the behaviour for translation through the `Translator` class. Each language is defined by the way the bytes are unpacked (mask and position in the word) and the type of the value it will be cast to (`uint8`, `int8`, `int16`, etc).

- **--cfg**: path to the configuration file that will be used in the translation. This file must have a given format, similar to the `.yaml` files that may be found under `cfg/daq/mapping`. By default, the parser will first check `cfg/daq/mapping/{cfg}.yaml` if it doesn't exist, it will check `{cfg}`.
    > **NOTE**: This is required for `it` and `la`, data from `dtupy` have already all fields needed but it can still be provided to have access to more fields related to the OBDT setup configuration.

- **--buffer-size**: maximum size the buffer will grow to (in lines) until it is dumped to the output `.parquet` file. Since I/O is computationally costly, setting a low number will increase the run time; setting a high number, will increase the memory consumption. By default, this value is `100000` (100k), which is a compromise between memory usage and computing time.

- **--debug**: turns on the debugging mode. By default, this is turned off.


### Examples
#### Translating from Italian
```bash
python scripts/daq/translate.py examples/data/cosmic_test_0409_1 examples/data/cosmic -l it --cfg cosmic
```

This example takes cosmic ray data taken with the Slow Control Box `examples/data/cosmic_test_0409_1.txt` with an OBDT configured as described in the file `cfg/mapping/cosmic.yaml` and translates it from `it` (Italian, Padova's updated format), storing the tabular dataset in `examples/data/cosmic.parquet`.

#### Translating from Spanish
```bash
python scripts/daq/translate.py examples/dumps/dump_20241122_144852/hits examples/data/dtupy -l es
```

This example takes data taken with `dtupy` in `examples/dumps/dump_20241122_144852` and translates it from `es` (Spanish, `dtupy` binary format), storing the tabular dataset in `examples/data/cosmic.parquet`.


> ### Tip: Partial translation!
> The program may be exited at any point of the loop raising a `KeyboardInterrupt` excpection (i.e., pressing `Ctrl+C`). This will be handled properly and the file will be saved up to the point of the interruption. This is useful if one wants to produce a partial translation on a reduced portion of the data.

## plot.py

This script takes the following arguments:

- **src_path**: path to the data file that will be translated. 
By default, the parser will first check `data/{src}` (in case the `src` has no suffix, the parser will look for `{src}.txt` instead). If this path doesn't exist, it will check `{src}`.
    > For example `file` will parse to `data/file.parquet` (or `file.parquet` if the previous path doesn't exist) and `.../dir/file` will parse to `.../dir/file.parquet`.

- **fig_dir**: directory where to save the drawn plots.
By default, this resolves to `figs`. A new folder with the name of the data file will be created under this directory to store all the generated plots. A new folder will be created as `{fig_dir}/{dataset name}/daq`, all plots will be stored inside.

- **--cfg**: path to the configuration file that will be used in the plotting. This file must have a given format, similar to the `.yaml` files that may be found under `.../cfg/daq`. By default, the parser will first check `.../cfg/daq/{cfg}.yaml` if it doesn't exist, it will check `{cfg}`. Default is `.../cfg/default.yaml`.
    - For files trasnlated from `es` please use `--cfg dtupy` as some fields may be called differently.


### Example
This example takes uses the data produced in the previous example for `translate.py` in Italian.
```bash
python plot.py examples/data/cosmic examples/figs
```


This example takes uses the data produced in the previous example for `translate.py` in Spanish.
```bash
python plot.py examples/data/dtupy examples/figs --cfg dtupy
```
