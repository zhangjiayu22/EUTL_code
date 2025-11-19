This repository provides a complete data processing pipeline for transforming the raw files downloaded from the
European Union Transaction Log (EUTL) into a structured, query-ready database. The project implements a multi-layered
data architecture (L0–L2) and outputs a clean, normalized dataset ready for empirical research on carbon market
operations.

1. Preparing the Raw Data
   (1)Download all official EUTL data files from the European Commission website.
   (2)Place all CSV files into the EUTL - code directory without modifying filenames. If the data is updated on the
   official website, you need to change the file names in '01 loading & create id.py'.
2. Running the Processing Pipeline
   (1)The processing workflow consists of ten scripts named in the format '01 xxx.py' to '10 xxx.py'
   (2)To execute the full pipeline, simply run the scripts in ascending numerical order.
   (3)Each script produces the intermediate output required by the next step.
   (4)By completing all ten steps sequentially, you will obtain all final datasets as defined in the project.
3. System Requirements
   (1)Mandatory Dependencies: MongoDB (Used as the primary backend for storing and querying intermediate datasets.)
   (2)Programming Environment: pymongo and pandas