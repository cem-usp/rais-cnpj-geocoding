# Load Packages -----

library(brandr)
library(downlit)
library(ggplot2)
library(here)
library(knitr)
library(magrittr)
library(quartor) # github.com/danielvartan/quartor
library(ragg)
library(showtext)
library(sysfonts)
library(xml2)

# Set General Options -----

options(
  dplyr.print_min = 6,
  dplyr.print_max = 6,
  pillar.max_footer_lines = 2,
  pillar.min_chars = 15,
  scipen = 10,
  digits = 10,
  stringr.view_n = 6,
  pillar.bold = TRUE,
  width = 77 # 80 - 3 for #> comment
)

# Set Variables -----

set.seed(2025)

# Set `knitr`` -----

clean_cache() |> suppressWarnings()

opts_chunk$set(
  comment = "#>",
  collapse = TRUE,
  root.dir = here(),
  dev = "ragg_png",
  dev.args = list(bg = "transparent"),
  fig.showtext = TRUE
)

# Set `brandr` -----

options(BRANDR_BRAND_YML = here("_brand.yml"))

brandr_options <- list(
  "BRANDR_COLOR_SEQUENTIAL" = get_brand_color(
    c("blue", "white")
  ),
  "BRANDR_COLOR_DIVERGING" =
    get_brand_color(c("comp-blue-3", "white", "comp-blue-6")),
  "BRANDR_COLOR_QUALITATIVE" = get_brand_color(
    c(
      "square-blue-green",
      "square-blue-orange",
      "square-blue-blue",
      "square-blue-purple",
      "gray",
      "black"
    )
  )
)

for (i in seq_along(brandr_options)) {
  options(brandr_options[i])
}

# Set `showtext` -----

font_paths(here("ttf"))

font_add(
  family = "montserrat",
  regular = here("ttf", "montserrat-regular.ttf"),
  bold = here("ttf", "montserrat-bold.ttf"),
  italic = here("ttf", "montserrat-italic.ttf"),
  bolditalic = here("ttf", "montserrat-bolditalic.ttf")
)

font_add(
  family = "roboto",
  regular = here("ttf", "roboto-regular.ttf"),
  bold = here("ttf", "roboto-bold.ttf"),
  italic = here("ttf", "roboto-italic.ttf"),
  bolditalic = here("ttf", "roboto-bolditalic.ttf")
)

showtext_auto()

# Set `ggplot2` -----

theme_set(
  theme_bw() +
    theme(
      text = element_text(
        color = get_brand_color("black"),
        family = "montserrat",
        face = "plain"
      ),
      panel.background = element_rect(fill = "transparent"),
      plot.background = element_rect(
        fill = "transparent", color = NA
      ),
      panel.grid.major = element_blank(),
      panel.grid.minor = element_blank(),
      legend.background = element_rect(fill = "transparent"),
      legend.box.background = element_rect(
        fill = "transparent", color = NA
      ),
      legend.frame = element_blank(),
      legend.ticks = element_line(color = "white")
    )
)

# Run `rbbt` -----

## Uncheck the option "Apply title-casing to titles" in Zotero Better BibTeX
## preferences (Edit > Settings > Better BibTeX > Miscellaneous).

# (2024-08-25)
# This function should work with any version of BetterBibTeX (BBT) for Zotero.
# Verify if @wmoldham PR was merged in the `rbbt` package (see issue #47
# <https://github.com/paleolimbot/rbbt/issues/47>). If not, install `rbbt`
# from @wmoldham fork `remotes::install_github("wmoldham/rbbt", force = TRUE)`.

# bbt_write_quarto_bib(
#   bib_file = here("references.bib"),
#   dir = c("."),
#   pattern = "\\.qmd$",
#   wd = here()
# )
