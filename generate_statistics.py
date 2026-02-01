import pandas as pd
import os


def generate_comprehensive_stats():
    """Generate detailed statistics from cleaned data"""

    cleaned_dir = r"d:\quad99\Scappers\output\India\final_cleaned_outputs"

    print("=== Comprehensive Statistics Report ===\n")

    total_rows = 0
    all_main_brands = set()
    all_other_brands = set()
    all_combinations = set()
    worker_stats = {}

    for worker_num in range(1, 6):
        file_path = os.path.join(cleaned_dir, f"worker_{worker_num}_final_cleaned.csv")

        if not os.path.exists(file_path):
            continue

        df = pd.read_csv(file_path)

        # Calculate worker-specific stats
        main_brands = set(df[df['BrandType'] == 'MAIN']['BrandName'].unique())
        other_brands = set(df[df['BrandType'] == 'OTHER']['BrandName'].unique())
        combinations = set(df[['BrandName', 'Company']].apply(
            lambda x: f"{x['BrandName']} | {x['Company']}", axis=1
        ))

        worker_stats[worker_num] = {
            'rows': len(df),
            'main_brands': len(main_brands),
            'other_brands': len(other_brands),
            'combinations': len(combinations)
        }

        total_rows += len(df)
        all_main_brands.update(main_brands)
        all_other_brands.update(other_brands)
        all_combinations.update(combinations)

    # Print detailed report
    print("Worker-wise Statistics:")
    print("-" * 80)
    for worker_num, stats in worker_stats.items():
        print(f"Worker {worker_num}: {stats['rows']:,} rows | "
              f"{stats['main_brands']:,} MAIN | {stats['other_brands']:,} OTHER | "
              f"{stats['combinations']:,} combinations")

    print(f"\n{'='*80}")
    print("TOTAL STATISTICS:")
    print(f"{'='*80}")
    print(f"Total Rows: {total_rows:,}")
    print(f"Unique MAIN Brands: {len(all_main_brands):,}")
    print(f"Unique OTHER Brands: {len(all_other_brands):,}")
    print(f"Total Unique Brands: {len(all_main_brands) + len(all_other_brands):,}")
    print(f"Total Unique Combinations: {len(all_combinations):,}")

    overlap = all_main_brands.intersection(all_other_brands)
    print(f"Overlapping Brands: {len(overlap)}")


if __name__ == "__main__":
    generate_comprehensive_stats()
