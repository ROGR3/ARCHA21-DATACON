import pandas as pd


def read_large_csv_sample(file_path, chunksize=100000):
    print(f"Načítám ukázku z {file_path} po {chunksize} řádcích...")
    chunk_iter = pd.read_csv(
        file_path, chunksize=chunksize, encoding="utf-8", low_memory=False
    )
    # Vytáhnout první chunk a zobrazit základní info
    first_chunk = next(chunk_iter)
    print(f"Sloupce ({len(first_chunk.columns)}): {list(first_chunk.columns)}")
    print(f"Ukázka dat (prvních 5 řádků):")
    print(first_chunk.head())
    print(
        f"Celkový počet řádků v souboru (přibližně): {sum(1 for _ in open(file_path, encoding='utf-8')) - 1}"
    )
    return first_chunk


def read_large_excel_sample(file_path, nrows=100):
    print(f"Načítám ukázku z Excel souboru {file_path} - prvních {nrows} řádků...")
    df = pd.read_excel(file_path, engine="openpyxl", nrows=nrows)
    print(f"Sloupce ({len(df.columns)}): {list(df.columns)}")
    print(f"Ukázka dat:")
    print(df.head())
    return df


def main():
    # CSV preskladane (připravené pro analýzu)
    DATA_FOLDER = "DATACON_data"
    cpzp_preskladane_file = f"{DATA_FOLDER}/CPZP_preskladane.csv"
    ozp_preskladane_file = f"{DATA_FOLDER}/OZP_preskladane.csv"
    kortizonove_ekv_file = f"{DATA_FOLDER}/kortizonove_ekvivalenty.csv"
    cpzp_raw_file = f"{DATA_FOLDER}/CPZP_raw_data.xlsx"
    ozp_raw_file = f"{DATA_FOLDER}/OZP_raw_data.txt"

    # Načteme vzorky z preskládaných CSV
    print("\n--- CPZP_preskladane.csv ---")
    cpzp_preskladane_sample = read_large_csv_sample(cpzp_preskladane_file)

    print("\n--- OZP_preskladane.csv ---")
    ozp_preskladane_sample = read_large_csv_sample(ozp_preskladane_file)

    print("\n--- kortizonove_ekvivalenty.csv ---")
    kort_ekv_sample = read_large_csv_sample(kortizonove_ekv_file)

    # Načteme vzorek z Excelu
    print("\n--- CPZP_raw_data.xlsx ---")
    cpzp_raw_sample = read_large_excel_sample(cpzp_raw_file)

    # Načteme pár řádků z textového souboru (OZP_raw_data.txt)
    print("\n--- OZP_raw_data.txt ---")
    with open(ozp_raw_file, encoding="cp1250") as f:
        for i in range(20):
            print(f.readline().strip())


if __name__ == "__main__":
    main()
