"""A very simple Streamlit app that connects to a MongoDB database and displays the data from the RawFile collection."""

import os

import streamlit as st
from service.components import show_sandbox_message
from service.utils import _log

from shared.keys import ALLOWED_CHARACTERS_IN_RAW_FILE_NAME, EnvVars

_log(f"loading {__file__}")

st.set_page_config(page_title="AlphaKraken: home", layout="wide")

"""
# Welcome to AlphaKraken!
"""

if os.environ.get(EnvVars.ENV_NAME) == "production":
    st.warning("""

    Note: you are currently viewing the very first version of the AlphaKraken.
    Bear in mind that the project is far from complete in terms of features.

    If you are missing something or have a cool idea or found a bug, please let us know: <support_email>
    """)
else:
    show_sandbox_message()

c1, _ = st.columns([0.5, 0.5])

c1.markdown("""### What is AlphaKraken?

This tool should help you keep track of your acquisitions and monitor their status and quality in (near) real-time.
By default, every single acquisition is processed by AlphaDIA and the results are stored in a database.
The processing is done on the cluster, which means that on rare occasions, it might be delayed due to high load.""")

c1.markdown("""### How to use it?

The "overview" tab shows all results, allows for filtering and sorting, and provides some basic quality measure plots.

The "status" tab shows the current status of the acquisition pipeline and the status of the last processed files.
It is mostly relevant for AlphaKraken admin users.

The "project" and "settings" tabs allow to manage specific AlphaDIA settings for certain raw files.
Currently they are meant to be used by AlphaKraken admin users only.""")

c1.markdown(f"""### Rules

To ensure a smooth automated processing, please follow these rules:
- Do NOT do anything (!) on the acquisition folder (=the folder where the raw files are written to). In particular:
**Do not _create_, _move_, _rename_, or _delete_ any files there**! Avoid opening them in any software (wait until the file is moved to the
"Backup" subfolder and open it there).
- Make sure to your raw filename does not contain any special characters. Only allowed: `{ALLOWED_CHARACTERS_IN_RAW_FILE_NAME}`
(basic latin letters, numbers, and a few special characters like `-`, `_`, `+`). Otherwise, they will not be quanted.
- If your file name contains `_dda_`, they will also not be quanted.""")

c1.markdown("""### FAQ
Q: A lot of jobs are stuck in status "quanting" or "queued_for_quanting".

A: This is the case when the cluster is under heavy load. The jobs will be processed as soon as possible.
Currently, status "quanting" means "quanting job submitted", regardless if it's still PENDING or already RUNNING.


Q: I am tired of always filling the filter, can this be saved?

A: Yes. Just add `?filter=value` to the URL to pre-fill the filter and then create a browser bookmark.
For technical reasons, combining multiple conditions is done using "AND" or "%26", not "&" like in the UI.
Combine with the `max_age=` parameter like so: `?filter=value1ANDvalue2&max_age=60`.


Q: The data on the overview page seems to be limited, can I see more?

A: For performance reasons, by default only recent data are loaded, and the table is truncated (plots and csv download
are not). If you really need to see more data, use the `?max_age=` and `?max_table_len=` query parameters in the URL (cf. above).



Q: I am missing a feature or found a bug or find the AlphaKraken unintuitive to use or want to contribute.

A: Please get in touch: [<support_email>](<support_email>)


Q: I want to know more about the AlphaKraken.

A: Have a look at the [github repo](https://github.com/MannLabs/alphakraken) or the underlying
Airflow implementation [here](http://<kraken_url>:8080) (read-only, ask for credentials).
""")
