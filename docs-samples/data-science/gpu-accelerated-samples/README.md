# GPU-Accelerated Samples

Jupyter notebooks demonstrating GPU acceleration using NVIDIA RAPIDS libraries on Microsoft Fabric and Azure VMs with NVIDIA GPUs.

> Based on examples from [NVIDIA RAPIDS](https://github.com/rapidsai) (Apache 2.0 License, Copyright NVIDIA Corporation). Modified and tested for Azure GPU VM environment.

## Requirements

- NVIDIA GPU (Tesla T4 or better)
- CUDA 12.x
- Python 3.12+
- RAPIDS 25.x (cuDF, cuML, cuCIM)
- PyOD (`pip install pyod`) for anomaly detection demo
- Conda environment recommended

## Notebooks

| Notebook | Description |
|----------|-------------|
| [rapids-gpu-accelerated-demo.ipynb](rapids-gpu-accelerated-demo.ipynb) | End-to-end GPU vs CPU benchmark: DataFrames, strings, KMeans, Random Forest, text embeddings, and KNN search |
| [cudf-pandas-stock-analysis.ipynb](cudf-pandas-stock-analysis.ipynb) | Stock market data analysis using GPU-accelerated pandas (read, merge, resample, plot) |
| [cuml-scikit-learn-accelerator-demo.ipynb](cuml-scikit-learn-accelerator-demo.ipynb) | Drop-in GPU acceleration for scikit-learn (PCA, UMAP, KNN, HDBSCAN on activity recognition data) |
| [pyod-gpu-anomaly-detection-demo.ipynb](pyod-gpu-anomaly-detection-demo.ipynb) | GPU-accelerated anomaly detection with PyOD and cuml.accel (IForest, PCA, KNN, CBLOF, HDBSCAN) |

### Notes

- **cudf-pandas-stock-analysis.ipynb** uses `%load_ext cudf.pandas` which must be the very first executed statement before any `import pandas`. In Fabric, pandas may be pre-imported by the environment — if so, add `cudf.pandas` to the Fabric session pre-run script.
- **cuml-scikit-learn-accelerator-demo.ipynb** downloads the UCI HAR dataset to `/tmp/HAR_data/` on first run (requires internet access).
- **cudf-pandas-stock-analysis.ipynb** downloads stock price data from Yahoo Finance (requires internet access).
- **pyod-gpu-anomaly-detection-demo.ipynb** uses `cuml.accel` for GPU acceleration. Dataset is synthetic (generated in-notebook). Increase `N_TRAIN` to 100K+ for meaningful GPU speedups.

## cuCIM Medical Imaging

GPU-accelerated image processing for digital pathology and microscopy using [cuCIM](https://github.com/rapidsai/cucim).

> Based on examples from [rapidsai/cucim](https://github.com/rapidsai/cucim) (Apache 2.0 License, Copyright NVIDIA Corporation). Modified for Azure GPU VM environment.

| Notebook | Description |
|----------|-------------|
| [01-whole-slide-image-reading.ipynb](cucim_medical_imaging/01-whole-slide-image-reading.ipynb) | Reading and displaying multi-resolution whole-slide pathology images |
| [02-image-cache-performance.ipynb](cucim_medical_imaging/02-image-cache-performance.ipynb) | Tile caching strategies for faster repeated region access |
| [03-gabor-texture-classification.ipynb](cucim_medical_imaging/03-gabor-texture-classification.ipynb) | GPU vs CPU Gabor filter bank for texture classification |
| [04-random-walker-segmentation.ipynb](cucim_medical_imaging/04-random-walker-segmentation.ipynb) | GPU-accelerated random walker image segmentation |
| [05-vesselness-filter.ipynb](cucim_medical_imaging/05-vesselness-filter.ipynb) | GPU-accelerated vessel detection using Frangi/Sato filters |

### Setup for cuCIM notebooks

**Notebooks 01 and 02** auto-download a sample whole-slide image (~170MB from OpenSlide) on first run. No manual setup needed.

**Notebooks 03, 04, and 05** are self-contained — they use synthetic data from scikit-image and do not require any additional files.
