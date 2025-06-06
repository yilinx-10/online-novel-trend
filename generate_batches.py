import argparse
import csv

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate and Distribute Bookbase URL Links in batch_i.json files.")
    parser.add_argument('--num_batches', type=int, default=8,
                        help='Number of json files to store the links.')
    args = parser.parse_args()

    import csv

    # Get and validate user inputs
    start_year = int(input("Enter start year (2003–2024): \n"))
    end_year = int(input("Enter end year (2003–2024): \n"))
    originality = int(input("Enter originality (All - 0, Original - 1, or Fan-fiction - 2): \n"))
    romance_type = int(input("Enter romance type (All - 0, Heterosexual - 1, Gay - 2, Lesbian - 3, Aromantic - 4, Multi - 5): \n"))

    if not (2003 <= start_year <= 2024 and 2003 <= end_year <= 2024 and start_year <= end_year):
        raise ValueError("Start and end year must be between 2003 and 2024, and start must be <= end.")
    if originality not in [0, 1, 2]:
        raise ValueError("Originality must be 0, 1, or 2.")
    if romance_type not in range(6):
        raise ValueError("Romance type must be 0, 1, 2, 3, 4, or 5.")

    # Generate argument combinations
    rows = []
    for year in range(start_year, end_year + 1):
        for page in range(1, 11):
            row = {
                "year": year,
                "originality": originality,
                "romance": romance_type,
                "pagenumber": page
            }
            rows.append(row)
    
    total = len(rows)
    n = total // args.num_batches
    rem = total % args.num_batches
    split_list = []
    start = 0

    for i in range(args.num_batches):
        end = start + n + (1 if i < rem else 0)
        split_list.append(rows[start:end])
        start = end

    for i, partial_list in enumerate(split_list):
        filename = f"page_batch_{i+1}.csv"
        with open(filename, mode="w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["year", "originality", "romance", "pagenumber"])
            writer.writeheader()
            writer.writerows(partial_list)
