# `Heasarc.locate_data` returns an empty table for every query

`locate_data` filters the datalink response on `content_type == 'directory'`, but the
HEASARC datalink service now labels the observation-directory row `text/html`. Nothing
survives the filter, so `locate_data` returns an empty table for every catalogue I tried.

This used to work; no code change on my side is involved.

## Reproducer

```python
from astroquery.heasarc import Heasarc

tab = Heasarc.query_tap(
    "SELECT obsid, __row FROM public.numaster WHERE obsid='80002092003'").to_table()
print(len(tab))                                        # 1

links = Heasarc.locate_data(tab, catalog_name="numaster")
print(len(links))                                      # 0  <-- expected 1
```

Expected: one row, with `access_url`, `sciserver` and `aws` pointing at the observation
directory. Actual: an empty table, so any `links[0]` downstream raises
`IndexError: index 0 out of range for table with length 0`.

Not mission-specific — same result for every catalogue I checked:

| catalogue | obsid | query rows | `locate_data` rows |
|---|---|---|---|
| `numaster` | `80002092003` | 1 | 0 |
| `nicermastr` | `1104010106` | 1 | 0 |
| `xtemaster` | `10408-01-01-00` | 1 | 0 |
| `chanmaster` | `1843` | 1 | 0 |

## What the service returns

Querying the same datalink endpoint directly with pyvo, for `numaster` / `80002092003`:

```
semantics=...products.jsp#nustar.obs  content_type='application/x-votable+xml;content=datalink'
    https://heasarc.gsfc.nasa.gov/xamin/vo/datalink?id=ivo://nasa.heasarc/numaster?...

semantics=...products.jsp#nustar.obs  content_type='text/html'
    https://heasarc.gsfc.nasa.gov/FTP/nustar/data/obs/00/8//80002092003/
```

The second row is the data directory, and it is the one `locate_data` is meant to keep.
Its `content_type` is `text/html`, not `directory`.

## Cause

`astroquery/heasarc/core.py`, in `locate_data` (unchanged in `main` as well as in the
0.4.11 release):

```python
dl_result = dl_result[np.ma.mask_or(
    dl_result['content_type'] == 'directory',
    dl_result['error_message'] != '',
    shrink=False
)]
```

## Suggested fix

`content_type == 'text/html'` on its own is not a safe replacement: for `chanmaster`,
eight rows come back and seven of them are `text/html` — bibliography, proposal
abstract, and "nearby observations" cross-links to other mission catalogues.

Two discriminators do look robust across the catalogues I tested:

1. **`semantics`** — the observation rows carry
   `.../products.jsp#<mission>.obs`, while the cross-links and bibliography rows carry
   `#link.<catalogue>`, `#point.bib`, `#<mission>.cxc.abs` and so on.
2. **`'/FTP/' in access_url`** — this is already how `locate_data` derives the
   `sciserver` and `aws` columns a few lines further down, so the rest of the method
   keeps working unchanged once the row is kept.

Selecting on `'/FTP/' in access_url` gives exactly the one correct row for every
catalogue in the table above.

It may also be worth not depending on `content_type` at all here, given that the value
is server-controlled and evidently not stable.

## Environment

```
astroquery  0.4.11   (also present in main)
pyvo        1.9.1
astropy     8.0.1
numpy       2.5.2
Python      3.14.4 (conda-forge), macOS arm64
```
