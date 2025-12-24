import re

GENERIC_RE = re.compile(r"^[A-Za-zÀ-ÖØ-öø-ÿ0-9'(),.\- /]+\s*:\s*$")

test_lines = [
    "ADALIMUMAB (HUMIRA) :",
    "GLATIRAMÈRE (acétate de) (COPAXONE):",
    "DENOSUMAB (PROLIA) :",
    "TOCILIZUMAB (ACTEMRA), Sol. Perf. I.V. :",
    "TOCILIZUMAB (ACTEMRA), Sol. Inj. S.C. (ser) et Sol. Inj. S.C. (stylo) :"
]

for ln in test_lines:
    match = GENERIC_RE.match(ln)
    print(f"{repr(ln)}: {bool(match)}")
