# U.S. states annual population estimates
Some Python to create a tidy data file, [`us-states-pop-estimates.csv`](us-states-pop-estimates.csv), with annual population estimates for U.S. states from 1970-present.

Data comes from the [U.S. Census Bureau tables of annual population estimates](https://www2.census.gov/programs-surveys/popest/). The full list of sources is in [`data-sources.json`](data-sources.json).

The data files come in a variety of formats, and the functions to download each file (into the `raw-data` directory) and parse the data into a common format are in [`get_pop_data.py`](get_pop_data.py).

### Run the code
- Clone or download this repository and `cd` into the directory
- Install [the requirements](requirements.txt) (`requests`, `us`, `openpyxl`) into a virtual environment using your tooling of choice
- `python get_pop_data.py`
