from docling.document_converter import DocumentConverter
import pandas as pd
from pathlib import Path
import sys

def main():
    # Настройки путей
    source_dir = Path("source")
    result_dir = Path("result")
    
    # Создаем папку для результатов, если её нет
    result_dir.mkdir(parents=True, exist_ok=True)
    
    # Ищем все PDF файлы
    pdf_files = list(source_dir.glob("*.pdf"))
    
    if not pdf_files:
        print(f"В папке {source_dir} не найдено PDF файлов.")
        return

    print(f"Найдено файлов для конвертации: {len(pdf_files)}")
    
    try:
        # Инициализируем конвертер один раз для всех файлов
        converter = DocumentConverter()
        
        for pdf_path in pdf_files:
            # Формируем имя выходного файла: имя_converted.xlsx
            output_file_name = f"{pdf_path.stem}_converted.xlsx"
            output_path = result_dir / output_file_name
            
            print(f"Обработка {pdf_path.name} -> {output_file_name}...")
            
            try:
                result = converter.convert(pdf_path)
                doc = result.document
                
                with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                    table_count = 0
                    if hasattr(doc, 'tables') and doc.tables:
                        for i, table in enumerate(doc.tables):
                            df = table.export_to_dataframe(doc)
                            sheet_name = f"Table {i+1}"
                            df.to_excel(writer, sheet_name=sheet_name, index=False)
                            table_count += 1
                        print(f"  Готово: извлечено {table_count} таблиц.")
                    else:
                        # Если таблиц нет, создаем информационный лист
                        df_empty = pd.DataFrame({"Сообщение": ["Таблицы не найдены в исходном документе."]})
                        df_empty.to_excel(writer, sheet_name="No Tables", index=False)
                        print("  Предупреждение: таблицы не найдены.")
            
            except Exception as e:
                print(f"  Ошибка при обработке {pdf_path.name}: {e}")

    except Exception as e:
        print(f"Ошибка при инициализации конвертера: {e}")
        sys.exit(1)

    print("\nВсе задачи завершены.")

if __name__ == "__main__":
    main()
