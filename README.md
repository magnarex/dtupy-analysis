# DT Upgrade Analysis toolkit (dtupy_analysis)


This package is a toolbox based on the previous software developed for the commissioning, performance study and certification of a Muon Telescope at CIEMAT [[1]](#1).

## Instalation
The `environment.yaml` file must be used for building a conda environment that can run the package in Python 3.12 as follows. This, and other required steps for setting up the package are done through:

```bash
source setup.sh
```

This will also add the path to the `src` folder to the `$PYTHONPATH` environment variable so that it can be used as any other package.


> **NOTE:** This will asume that `conda` is a recognized command and will create a conda environment called `dtupy-analysis`. If you need to build the environment in a different way, please run instead:
> ```bash
> source setup.sh --no-conda
> ```

## References
<a id="1">[1]</a>
Alcalde, M. (2023) [*Commissioning, performance studies and certification of a Drift Tubes Muon Telescope*](http://cfp.ciemat.es/tfmmartinalcalde). 
