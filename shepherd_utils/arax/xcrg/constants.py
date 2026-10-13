# Vendored from Translator-CATRAX/xCRG @ e67f2c0, src/xcrg/constants.py (DEC-21).
# Changes from upstream:
#   - unchanged
# See README.md.
DEFAULT_TF_FILE = "transcription_factors.json"
DIRECT_QEDGE_ID = "direct"
TF_QNODE_ID = "tf"
# TP53 is a "master regulator" and regulates hundreds of genes.
# We were finding it showed up everywhere (in paths of all lengths).
# It's manually removed so we don't drown in X<-TP53->Y paths.
# Maybe with better ranking, we could leave it in there, but for now, it's manually removed
TP53_CURIE = "NCBIGene:7157"
