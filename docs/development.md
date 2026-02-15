# DAGBench Development Notes

## Lessons Learned

### Citation Integrity Incident (2026-02-14)

**Severity:** Critical. 7 citation errors found across 15+ metadata files, all introduced by Claude during initial workflow imports.

#### What Happened

During the original creation of DAGBench workflows, Claude generated bibliographic metadata (paper titles, author names, DOIs, arXiv IDs) from parametric memory rather than verifying against authoritative sources. This produced plausible-looking but factually wrong citations that passed casual inspection.

#### Errors Found

| Scope | Error | Detail |
|-------|-------|--------|
| PISA paper (6 synthetic workflows + import script) | Hallucinated title and authors | Generated "A Comprehensive Benchmark for Scheduling Algorithms" by "Justin Boggs, Landon Sights, Bhaskar Krishnamachari". Actual: "An Adversarial Approach To Comparing Task Graph Scheduling Algorithms" by Jared Coleman, Bhaskar Krishnamachari. "Justin Boggs" and "Landon Sights" do not exist. |
| RIoTBench (4 workflows) | Fabricated DOI | `10.1109/SC2.2017.42` looks like a valid IEEE DOI but resolves to an unrelated paper. Actual DOI: `10.1002/cpe.4257` (Wiley). |
| Loki traffic pipeline | All authors fabricated | Found the real DOI but generated 4 completely wrong author names. |
| Autonomous driving | Wrong authors | Real DOI, hallucinated author list. |
| Face recognition | Wrong authors | Real DOI, hallucinated author list. |
| Face analysis pipeline | Wrong title, missing authors | Paraphrased instead of exact title; omitted authors. |
| SFC 5G | First author name wrong | "Henry Ijemaru Adoga" vs. actual "Haruna Umar Adoga". |

#### Why It Happened

1. **Memory-based generation**: Claude's parametric memory produces confident-sounding but unreliable bibliographic details. Author names, paper titles, and DOIs are especially prone to hallucination because they look plausible even when wrong.
2. **No verification step**: The original import scripts were written and run without a cross-check against the actual papers/DOIs.
3. **Plausibility trap**: Fabricated DOIs used correct publisher prefixes (e.g., `10.1109/` for IEEE), fabricated authors had realistic names, and fabricated titles were topically relevant. Nothing looked obviously wrong.

#### Prevention Rules (Mandatory for DAGBench)

1. **Never generate bibliographic metadata from memory.** Every DOI, arXiv ID, paper title, and author list MUST be fetched from the actual source (Crossref API, arXiv, publisher page).
2. **Verify every DOI resolves correctly.** Use `https://doi.org/{doi}` or `https://api.crossref.org/works/{doi}` to confirm the DOI maps to the intended paper.
3. **Copy-paste, don't paraphrase.** Paper titles must be exact character-for-character matches. Author names must match the published form.
4. **Audit before commit.** Any batch import of citations must include a verification step that checks each citation against its source before the commit is made.
5. **When in doubt, leave it blank.** An empty `paper_doi` field is infinitely better than a fabricated one. Use the `notes` field to say "DOI not yet verified" rather than guessing.

#### How the Errors Were Fixed

A comprehensive audit was conducted on 2026-02-14 using parallel web searches and DOI resolution checks against Crossref, arXiv, IEEE Xplore, Springer, Wiley, and MDPI. All 7 errors were corrected across metadata.yaml files, docs.html files, import scripts, README.md, sources.md, and index.html. Total: 45 files modified in a single commit.
