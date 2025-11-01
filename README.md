# rais-cnpj-geocoding

<!-- badges: start -->
[![Project Status: Active – The project has reached a stable, usable state and is being actively developed.](https://www.repostatus.org/badges/latest/active.svg)](https://www.repostatus.org/#active)
[![License: GPLv3](https://img.shields.io/badge/license-GPLv3-bd0000.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![License: CC BY-NC-SA 4.0](https://img.shields.io/badge/license-CC_BY--NC--SA_4.0-lightgrey.svg)](https://creativecommons.org/licenses/by-nc-sa/4.0/)
<!-- badges: end -->

## Overview

This repository provides a reproducible pipeline for processing and [geocoding](https://en.wikipedia.org/wiki/Address_geocoding) [CNPJ](https://en.wikipedia.org/wiki/CNPJ)s from the Annual Social Information Report ([RAIS](https://www.rais.gov.br/)) of the Brazilian Ministry of Labor and Employment ([MTE](https://www.gov.br/trabalho-e-emprego)). The pipeline was developed by **Gabriel Caldeira** and **Clara Penz**, with further adaptations by [**Daniel Vartanian**](https://github.com/danielvartan).

The report is available [here](https://cem-usp.github.io/rais-cnpj-geocoding/).

> If you find this project useful, please consider giving it a star! [![GitHub repo stars](https://img.shields.io/github/stars/cem-usp/logo-pattern)](https://github.com/cem-usp/rais-cnpj-geocoding/)

## Data Availability

[![OSF ID](https://img.shields.io/badge/OSF%20ID-p5ufj-1284C5.svg)](https://osf.io/p5ufj/)

The processed data are available in both [`rds`](https://rdrr.io/r/base/readRDS.html) and [`parquet`](https://en.wikipedia.org/wiki/Apache_Parquet) formats through a dedicated repository on the Open Science Framework ([OSF](https://osf.io/)). A metadata file is included alongside the validated datasets.

Because the raw data are not publicly available, only authorized personnel can access the processed files. They are protected with [RSA](https://en.wikipedia.org/wiki/RSA_cryptosystem) 4096-bit encryption ([OpenSSL](https://www.openssl.org/)) and a 32-byte password to ensure data security.

If you already have access to the OSF repository and the project keys, click [here](https://osf.io/) to access the data. A backup copy of the raw data is also stored on OSF and can be accessed [here](https://osf.io/tdswp/). You can also retrieve these files directly from [R](https://www.r-project.org/) using the [`osfr`](https://docs.ropensci.org/osfr/) package.

## Usage

The pipeline was developed using the [Quarto](https://quarto.org/) publishing system and the [R programming language](https://www.r-project.org/). To ensure consistent results, the [`renv`](https://rstudio.github.io/renv/) package is used to manage and restore the R environment.

Access to the raw data is restricted, as the dataset was obtained upon request and cannot be publicly shared. Running the analyses requires an active internet connection and a set of access keys (see the [*Keys*](#keys) section). Do not use VPNs, corporate proxies, or other network-routing tools while processing the data, as these can interfere with authentication and downloads.

After installing the three dependencies mentioned above and setting all the keys, follow these steps to reproduce the analyses:

1. **Clone** this repository to your local machine.
2. **Open** the project in your preferred IDE.
3. **Restore the R environment** by running [`renv::restore()`](https://rstudio.github.io/renv/reference/restore.html) in the R console. This will install all required software dependencies.
4. **Open** `index.qmd` and run the code as described in the report.

## Keys

To access the data and run the [Quarto](https://quarto.org/) notebook, you must first obtain authorization to access the project's [OSF](https://osf.io) repositories and [Google Sheets](https://workspace.google.com/products/sheets/) files.

Once you have the necessary permissions, run the following command to authorize your access to the Google Sheets API:

```r
library(gargle)
library(googlesheets4)

options(gargle_oauth_cache = ".secrets")

gs4_auth()
gargle_oauth_cache()
```

Next, create a file named [`.Renviron`](https://bookdown.org/csgillespie/efficientR/set-up.html#:~:text=2.4.6%20The%20.Renviron%20file) in the root directory of the project and add the following environment variables:

- `OSF_PAT`: Your [OSF](https://osf.io/) Personal Access Token ([PAT](https://en.wikipedia.org/wiki/Personal_access_token)). If you don't have one, go to the settings section of your OSF account and create a new token.
- `ACESSOSAN_PASSWORD`: The password for the project's [RSA](https://en.wikipedia.org/wiki/RSA_cryptosystem) private key (32 bytes).

Example (do not use these values):

```ini
OSF_PAT=bWHtQBmdeMvZXDv2R4twdNLjmakjLUZr4t72ouAbNjwycGtDzfm3gjz4ChYXwbBaBVJxJR
ACESSOSAN_PASSWORD=MmXN_od_pe*RdHgfKTaKiXdV7KD2qPzW
```

Additionally, you will need the following keys in the project's [`_ssh`](_ssh) folder:

- `id_rsa`: The project's private RSA key ([RSA](https://en.wikipedia.org/wiki/RSA_cryptosystem) 4096 bits (OpenSSL)).
- `id_rsa.pub`: The project's public RSA key.

These project's keys are provided to authorized personnel only. If you need access, please contact the authors.

## Known Issues

### Arrow Dependencies

```
Error in `dplyr::compute()`:
! NotImplemented: Support for codec 'zstd' not built
```

This error occurs when [`arrow`](https://arrow.apache.org/docs/r/index.html) is missing certain dependencies. To fix it, run:

```r
Sys.setenv(LIBARROW_MINIMAL = "false")
```

Then reinstall the `arrow` package:

```r
install.packages("arrow")
```

## Citation

> [!IMPORTANT]
> When using this data, you must also cite the original data sources.

To cite this work, please use the following format:

Caldeira, G., Penz, C., Vartanian, D., Fernandes, C. N., & Giannotti, M. A. (2025). *A reproducible pipeline for processing and geocoding CNPJs from the Annual Social Information Report (RAIS) of the Brazilian Ministry of Labor and Employment (MTE)* \[Computer software\]. Center for Metropolitan Studies of the University of São Paulo. <https://cem-usp.github.io/rais-cnpj-geocoding>

A BibLaTeX entry for LaTeX users is

```latex
@software{caldeira2025,
  title = {A reproducible pipeline for processing and geocoding CNPJs from the Annual Social Information Report (RAIS) of the Brazilian Ministry of Labor and Employment (MTE)},
  author = {{Gabriel Caldeira} and {Clara Penz} and {Daniel Vartanian} and {Camila Nastari Fernandes} and {Mariana Abrantes Giannotti}},
  year = {2025},
  address = {São Paulo},
  institution = {Center for Metropolitan Studies of the University of São Paulo},
  langid = {en},
  url = {https://cem-usp.github.io/rais-cnpj-geocoding}
}
```

## License

[![License: GPLv3](https://img.shields.io/badge/license-GPLv3-bd0000.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![License: CC BY-NC-SA 4.0](https://img.shields.io/badge/license-CC_BY--NC--SA_4.0-lightgrey.svg)](https://creativecommons.org/licenses/by-nc-sa/4.0/)

> [!IMPORTANT]
> The original data sources may be subject to their own licensing terms and conditions.

The code in this repository is licensed under the [GNU General Public License Version 3](https://www.gnu.org/licenses/gpl-3.0), while the report is available under the [Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International](https://creativecommons.org/licenses/by-nc-sa/4.0/).

``` text
Copyright (C) 2025 Center for Metropolitan Studies

The code in this report is free software: you can redistribute it and/or
modify it under the terms of the GNU General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your option)
any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program. If not, see <https://www.gnu.org/licenses/>.
```

## Acknowledgments

<table>
  <tr>
    <td width="30%">
      <br>
      <p align="center">
        <a href="https://doi.org/10.17605/OSF.IO/ZE6WT"><img src="images/acessosan-logo.svg" width="140em"/></a>
      </p>
      <br>
    </td>
    <td width="70%">
      This work is part of a research project by the Polytechnic School (<a href="https://www.poli.usp.br/">Poli</a>) of the University of São Paulo (<a href="https://usp.br/">USP</a>), in partnership with the Secretariat for Food and Nutrition Security (<a href="https://www.gov.br/mds/pt-br/orgaos/SESAN">SESAN</a>) of the Ministry of Social Development, Family, and the Fight Against Hunger (<a href="https://www.gov.br/mds/">MDS</a>), titled: <em>AcessoSAN: Mapping Food Access to Support Public Policies on Food and Nutrition Security and Hunger Reduction in Brazilian Cities</em>.
    </td>
  </tr>
</table>

<table>
  <tr>
    <td width="30%">
      <br>
      <p align="center">
        <a href="https://centrodametropole.fflch.usp.br"><img src="images/cem-icon.svg" width="190em"/></a>
      </p>
      <br>
    </td>
    <td width="70%">
      This work was developed with support from the Center for Metropolitan Studies (<a href="https://centrodametropole.fflch.usp.br">CEM</a>) based at the School of Philosophy, Letters and Human Sciences (<a href="https://www.fflch.usp.br/">FFLCH</a>) of the University of São Paulo (<a href="https://usp.br">USP</a>) and at the Brazilian Center for Analysis and Planning (<a href="https://cebrap.org.br/">CEBRAP</a>).
    </td>
  </tr>
</table>

<table>
  <tr>
    <td width="30%">
      <br>
      <p align="center">
        <br> <a href="https://fapesp.br/"><img src="images/fapesp-logo.svg" width="160em"/></a>
      </p>
      <br>
    </td>
    <td width="70%">
      This study was financed, in part, by the São Paulo Research Foundation (<a href="https://fapesp.br/">FAPESP</a>), Brazil. Process Number <a href="https://bv.fapesp.br/en/bolsas/231507/geospatial-data-science-applied-to-food-policies/">2025/17879-2</a>.
    </td>
  </tr>
</table>
