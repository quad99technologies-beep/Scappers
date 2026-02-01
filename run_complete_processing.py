from final_india_scrapper_processor import process_india_scrapper_data
from generate_statistics import generate_comprehensive_stats


if __name__ == "__main__":
    print("Starting complete India scrapper data processing...")

    # Process all data
    stats = process_india_scrapper_data()

    # Generate statistics
    generate_comprehensive_stats()

    print("\nComplete processing finished!")
