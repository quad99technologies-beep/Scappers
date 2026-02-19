import csv
import io

fieldnames = ["A", "B", "C", "D", "E"]
row = {"A": "1", "B": "2", "C": "3\t", "D": "4", "E": "5"}

# CSV with Tab in field C
out = io.StringIO()
writer = csv.DictWriter(out, fieldnames=fieldnames)
writer.writeheader()
writer.writerow(row)
print("CSV Output (Quoted):")
print(out.getvalue())

# If Tab is treated as delimiter by viewer
print("\nIf Tab is treated as delimiter by viewer:")
# C splitting into C and D
# C: 3
# D: (nothing)
# E: 4
# ...?
