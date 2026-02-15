# Refine Netherlands Combination Loader

- [ ] Create `01_load_combinations_smart.py` with the following logic:
    - [ ] Navigates to search page.
    - [ ] Extracts all Vorm options.
    - [ ] For each Vorm:
        - [ ] Selects Vorm in dropdown (triggers Sterkte update).
        - [ ] Waits for Sterkte dropdown to update.
        - [ ] Extracts specific Sterktes for this Vorm.
        - [ ] Generates combinations:
            - [ ] If 'Alle sterktes' < 5000 (check dynamically? unlikely to scrape 5000 just to check), assume safe for small categories.
            - [ ] For large categories (Tablets, etc.), generate granular combinations for EVERY Sterkte.
- [ ] Verify `01_load_combinations_smart.py` executes successfully.
- [ ] Confirm huge number of combinations (~100-200 for Tablets) are generated.
