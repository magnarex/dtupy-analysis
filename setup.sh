script_dir="$(dirname "$(realpath "${BASH_SOURCE[0]}")")"

echo "Creating batch subdirectories..."
mkdir -p "$script_dir/batch/jobs"; echo "  - $script_dir/batch/jobs"
mkdir -p "$script_dir/batch/logs"; echo "  - $script_dir/batch/logs"

# Definir el destino del enlace simbólico y el directorio de origen
target="$script_dir/scripts"
link="$script_dir/batch/scripts"

echo "Adding symbolic links for scripts to batch..."
# Verify wether the symbolic link already exists and points to the target
if [ -L "$link" ] && [ "$(readlink "$link")" = "$target" ]; then
    echo "  $target already linked to $link!"
    echo "  Skipping..."
else
    # Crear el enlace simbólico
    echo "  Linking: $link -> $target"
    ln -sfn "$target" "$link"
    echo "  Done!"
fi

# Create the conda env only if --no-conda is not provided
if [[ ! " $@ " =~ " --no-conda " ]]; then
    echo "Creating conda environment..."
    conda env create -f "$script_dir/environment.yml"
else
    echo "Skipping conda environment creation due to --no-conda option."
fi

if [[ ":$PYTHONPATH:" != *":$script_dir/src:"* ]]; then
    echo "Adding $script_dir/src to PYTHONPATH..."
    export PYTHONPATH="$script_dir/src:$PYTHONPATH"
else
    echo "$script_dir/src is already in PYTHONPATH."
fi