import sys
import os
import pandas as pd
from dataclasses import dataclass


class CSV_Combiner:

    @staticmethod
    def check_file_paths(argv):

        if len(argv) <= 1:
            print("Error: No file-paths input. Please run the code as follows: \n" +
                  "python ./csv_combiner.py ./fixtures/accessories.csv ./fixtures/clothing.csv > combined.csv")
            return False

        filelst = argv[1:]

        for file_path in filelst:
            if os.stat(file_path).st_size == 0:
                print("File is empty: " + file_path)
                return False
            if not os.path.exists(file_path):
                print("Not found: " + file_path)
                return False
        return True

    def combine_files(self, argv: list):

        chunksize = 100000  # Reading 100000 rows at once
        rows_list = []

        if self.check_file_paths(argv):
            filelst = argv[1:]

            for file_path in filelst:

                # chunksize option in pandas to ready in batches
                for rows in pd.read_csv(file_path, chunksize=chunksize):

                    filename = os.path.basename(file_path)

                    # creating now column with respective filename for rows
                    rows['filename'] = filename
                    rows_list.append(rows)

            header = True

            # combine all chunks
            for row in rows_list:
                print(row.to_csv(index=False, header=header,
                      line_terminator='\n', chunksize=chunksize), end='')
                header = False  # Only needed for the first file
        else:
            return


def main():
    combiner = CSVCombiner()
    combiner.combine_files(sys.argv)


if __name__ == '__main__':
    main()
