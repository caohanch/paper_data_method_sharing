# Tracking data and methods link sharing in scientific publications
Codes for paper "The Rise of Open Science: Tracking the Evolution and Perceived Value of Data and Methods Link-Sharing Practices" by Hancheng Cao, Jesse Dodge, Kyle Lo, Daniel A. McFarland, and Lucy Lu Wang.

parallel_link_detection_parent.py, parallel_link_extraction_child_timeout downloads arXiv full text files, unzip them, and use S2ORC Doc2json (https://github.com/allenai/s2orc-doc2json) to parse files in LaTex to json. Then the script use regular expression to retrieve links, and associted contexts.

link_classification.ipynb fine-tuned a SciBert model (https://github.com/allenai/scibert) to classify links into different functional types using labelled dataset by Zhao et al. https://aclanthology.org/D19-1524.pdf, https://github.com/zhaohe1995/SciRes.

Associated dataset we used to generate figures in the paper is available at:  https://doi.org/10.7910/DVN/674ZDJ, the analysis code is in analysis_script.ipynb.
