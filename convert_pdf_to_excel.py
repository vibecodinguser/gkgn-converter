from docling.document_converter import DocumentConverter
import pandas as pd
from pathlib import Path
import sys
import inspect

# Настройки трансформации данных
RENAME_MAP = {
    "Географические координаты.Широта.Привязка к другим географическим объектам": "DDD Широта",
    "Географические координаты.Долгота.Привязка к другим географическим объектам": "DDD Долгота",
    "Административно территориальная ( муниципальная привязка.Административно территориальная ( муниципальная привязка.Административно территориальная ( муниципальная привязка": "АТД",
    "Тип объекта.Тип объекта.Тип объекта": "Тип объекта",
    "Название географического объекта.Название географического объекта.Название географического объекта": "Название географического объекта"
}

DROP_COLUMNS = [
    "Регистр . номер.Регистр . номер.Регистр . номер",
    "Номенкл . листа карты масштаба 1 : 100 000.Номенкл . листа карты масштаба 1 : 100 000.Номенкл . листа карты масштаба 1 : 100 000"
]

FINAL_COLUMNS = [
    "Рег.номер",
    "Название",
    "Тип объекта",
    "АТД",
    "Широта",
    "Долгота",
    "Лист",
]

TEMP_DUP_SUFFIX = "__TMP_DUPCOL__"

def make_columns_unique(df):
    """Техническая функция для обеспечения уникальности имен столбцов перед конкатенацией."""
    if df.columns.duplicated().any():
        counts = {}
        unique_cols = []
        for col in map(str, df.columns):
            current_count = counts.get(col, 0)
            unique_cols.append(col if current_count == 0 else f"{col}{TEMP_DUP_SUFFIX}{current_count}")
            counts[col] = current_count + 1
        df.columns = pd.Index(unique_cols)
    return df


def concat_raw_tables(raw_tables):
    """Объединяет сырые таблицы; семантические преобразования выполняются только после concat."""
    prepared = [make_columns_unique(df.copy()) for df in raw_tables]
    return pd.concat(prepared, ignore_index=True)

def transform_dataframe(df):
    """
    ФУНКЦИЯ ТРАНСФОРМАЦИИ: выполняется строго ПОСЛЕ объединения всех листов.
    """
    # 1. Снимаем временные технические суффиксы, добавленные только для безопасного concat
    df.columns = pd.Index([str(col).split(TEMP_DUP_SUFFIX)[0] for col in df.columns])

    # 2. Очистка имен столбцов от технических символов (\n, лишние пробелы)
    df.columns = pd.Index([str(col).replace('\n', ' ').strip() for col in df.columns])

    # 3. Схлопывание дублирующихся по смыслу столбцов (напр. "Широта" и "Широта\n")
    if df.columns.duplicated().any():
        unique_cols = []
        seen = set()
        for col in df.columns:
            if col not in seen:
                unique_cols.append(col)
                seen.add(col)
        
        new_data = {}
        for col in unique_cols:
            subset = df.loc[:, df.columns == col]
            if subset.shape[1] > 1:
                # Объединяем данные дублирующихся столбцов
                new_data[col] = subset.bfill(axis=1).iloc[:, 0]
            else:
                new_data[col] = subset.iloc[:, 0]
        df = pd.DataFrame(new_data, index=df.index)

    # # 4. Удаление ненужных столбцов
    # cols_to_drop = [c for c in DROP_COLUMNS if c in df.columns]
    # if cols_to_drop:
    #     df = df.drop(columns=cols_to_drop)
    
    # 5. Переименование согласно словарю
    df = df.rename(columns=RENAME_MAP)
    
    # 6. Финальная дедупликация (если переименование создало одинаковые имена)
    if df.columns.duplicated().any():
        cols = pd.Series(df.columns)
        for dup in cols[cols.duplicated()].unique():
            cols[cols == dup] = [f"{dup}_{i}" if i != 0 else dup for i in range(sum(cols == dup))]
        df.columns = cols

    # 7. Очистка содержимого ячеек
    if hasattr(df, 'map'):
        df = df.map(lambda x: x.replace('\n', ' ') if isinstance(x, str) else x)
    else:
        df = df.applymap(lambda x: x.replace('\n', ' ') if isinstance(x, str) else x)

    # 8. После всех преобразований удаляем первые две строки
    df = df.iloc[2:].reset_index(drop=True)

    # 9. Нормализуем к 7 колонкам и задаем итоговые заголовки по порядку
    if df.shape[1] > len(FINAL_COLUMNS):
        df = df.iloc[:, :len(FINAL_COLUMNS)]
    elif df.shape[1] < len(FINAL_COLUMNS):
        for i in range(df.shape[1], len(FINAL_COLUMNS)):
            df[i] = pd.NA

    df.columns = FINAL_COLUMNS

    # 10. В столбцах Широта/Долгота удаляем все символы справа от апострофа
    def trim_after_apostrophe(value):
        if isinstance(value, str) and "'" in value:
            left = value.split("'", 1)[0]
            return f"{left}'"
        return value

    for coord_col in ("Широта", "Долгота"):
        if coord_col in df.columns:
            df[coord_col] = df[coord_col].map(trim_after_apostrophe)

    return df

def export_table_without_header(table, doc):
    """Извлекает DataFrame без использования строки заголовка таблицы."""
    export_fn = table.export_to_dataframe
    signature = inspect.signature(export_fn)
    if "header" in signature.parameters:
        return export_fn(doc, header=None)

    # Fallback для версий, где параметр header отсутствует
    df = export_fn(doc)
    df.columns = pd.RangeIndex(start=0, stop=df.shape[1], step=1)
    return df

def main():
    source_dir = Path("source")
    result_dir = Path("result")
    result_dir.mkdir(parents=True, exist_ok=True)
    
    pdf_files = list(source_dir.glob("*.pdf"))
    if not pdf_files:
        print(f"В папке {source_dir} не найдено PDF файлов.")
        return

    print(f"Найдено файлов для конвертации: {len(pdf_files)}")
    
    try:
        converter = DocumentConverter()
        
        for pdf_path in pdf_files:
            output_file_name = f"converted_{pdf_path.stem}.xlsx"
            output_path = result_dir / output_file_name
            
            print(f"Обработка {pdf_path.name}...")
            
            try:
                result = converter.convert(pdf_path)
                doc = result.document
                
                if hasattr(doc, 'tables') and doc.tables:
                    all_dfs = []
                    for table in doc.tables:
                        # ШАГ 0: Только извлечение сырого DataFrame (без манипуляций со столбцами)
                        df = export_table_without_header(table, doc)
                        all_dfs.append(df)
                    
                    if all_dfs:
                        # ШАГ 1: ОБЪЕДИНЕНИЕ всех сырых таблиц
                        print(f"  Объединение {len(all_dfs)} таблиц в одну кучу...", flush=True)
                        combined_df = concat_raw_tables(all_dfs)

                        # ШАГ 2: ТРАНСФОРМАЦИЯ столбцов только после полного объединения
                        print(f"  Трансформация объединенного массива данных...", flush=True)
                        final_df = transform_dataframe(combined_df)
                        
                        final_df.to_excel(output_path, sheet_name="MergedData", index=False)
                        print(f"  Готово: Данные сохранены в {output_file_name}", flush=True)
                    else:
                        print(f"  Не удалось извлечь таблицы в {pdf_path.name}")
                else:
                    print(f"  В файле {pdf_path.name} таблицы не обнаружены.")
            
            except Exception as e:
                print(f"  Ошибка при обработке {pdf_path.name}: {e}")

    except Exception as e:
        print(f"Ошибка при запуске конвертера: {e}")
        sys.exit(1)

    print("\nОбработка завершена.")

if __name__ == "__main__":
    main()
