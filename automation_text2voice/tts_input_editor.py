import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import pandas as pd
import os
import csv
import chardet
from pathlib import Path

class TTSInputEditor:
    def __init__(self, root):
        self.root = root
        self.root.title("TTS Input Editor")
        self.root.geometry("800x600")
        
        # Variables
        self.input_folder = tk.StringVar()
        self.output_file = tk.StringVar(value="all_scripts.csv")
        
        self.setup_ui()
        
    def setup_ui(self):
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        
        # Input folder selection
        ttk.Label(main_frame, text="Input Folder:").grid(row=0, column=0, sticky=tk.W, pady=5)
        ttk.Entry(main_frame, textvariable=self.input_folder, width=50).grid(row=0, column=1, sticky=(tk.W, tk.E), padx=5)
        ttk.Button(main_frame, text="Browse", command=self.browse_folder).grid(row=0, column=2, padx=5)
        
        # Output file name
        ttk.Label(main_frame, text="Output File:").grid(row=1, column=0, sticky=tk.W, pady=5)
        ttk.Entry(main_frame, textvariable=self.output_file, width=50).grid(row=1, column=1, sticky=(tk.W, tk.E), padx=5)
        
        # Process button
        ttk.Button(main_frame, text="Process CSV Files", command=self.process_files).grid(row=2, column=0, columnspan=3, pady=20)
        
        # Progress bar
        self.progress = ttk.Progressbar(main_frame, mode='indeterminate')
        self.progress.grid(row=3, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        
        # Status label
        self.status_label = ttk.Label(main_frame, text="Ready to process files")
        self.status_label.grid(row=4, column=0, columnspan=3, pady=5)
        
        # Text area for log
        self.log_text = tk.Text(main_frame, height=20, width=80)
        self.log_text.grid(row=5, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S), pady=10)
        
        # Scrollbar for text area
        scrollbar = ttk.Scrollbar(main_frame, orient=tk.VERTICAL, command=self.log_text.yview)
        scrollbar.grid(row=5, column=3, sticky=(tk.N, tk.S))
        self.log_text.configure(yscrollcommand=scrollbar.set)
        
        # Configure grid weights for main_frame
        main_frame.rowconfigure(5, weight=1)
        
    def browse_folder(self):
        folder = filedialog.askdirectory()
        if folder:
            self.input_folder.set(folder)
            
    def log_message(self, message):
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)
        self.root.update()
        
    def detect_encoding(self, file_path):
        """Detect file encoding"""
        with open(file_path, 'rb') as f:
            raw_data = f.read()
            result = chardet.detect(raw_data)
            return result['encoding']
            
    def read_csv_file(self, file_path):
        """Read CSV file with proper encoding detection"""
        try:
            encoding = self.detect_encoding(file_path)
            if encoding is None:
                encoding = 'utf-8'
            
            # Try different encodings
            encodings_to_try = [encoding, 'utf-8', 'utf-8-sig', 'cp1256', 'iso-8859-1']
            
            for enc in encodings_to_try:
                try:
                    df = pd.read_csv(file_path, encoding=enc, sep=';', header=None)
                    return df
                except:
                    continue
                    
            # If all fail, try with error handling
            df = pd.read_csv(file_path, encoding='utf-8', sep=';', header=None, encoding_errors='ignore')
            return df
            
        except Exception as e:
            self.log_message(f"Error reading {file_path}: {str(e)}")
            return None
            
    def process_csv_file(self, file_path, all_data):
        """Process a single CSV file"""
        self.log_message(f"Processing: {os.path.basename(file_path)}")
        
        df = self.read_csv_file(file_path)
        if df is None or df.empty:
            self.log_message(f"  Skipped - empty or unreadable file")
            return
            
        # Check if file has at least 3 rows
        if len(df) < 3:
            self.log_message(f"  Skipped - file has less than 3 rows")
            return
            
        # Get first column values for first 3 rows
        first_col = df.iloc[:, 0]
        val1 = str(first_col.iloc[0]) if pd.notna(first_col.iloc[0]) else ""
        val2 = str(first_col.iloc[1]) if pd.notna(first_col.iloc[1]) else ""
        val3 = str(first_col.iloc[2]) if pd.notna(first_col.iloc[2]) else ""
        
        self.log_message(f"  First column values: '{val1}', '{val2}', '{val3}'")
        
        # Determine which rows to include
        rows_to_include = []
        
        # Always include row 2 (index 2) and beyond
        for i in range(2, len(df)):
            rows_to_include.append(i)
            
        # Check if first row should be included
        if val1 == val3:
            rows_to_include.append(0)
            self.log_message(f"  Including first row (matches third)")
            
        # Check if second row should be included
        if val2 == val3:
            rows_to_include.append(1)
            self.log_message(f"  Including second row (matches third)")
            
        # Sort rows to maintain order
        rows_to_include.sort()
        
        # Extract data for included rows
        for row_idx in rows_to_include:
            row_data = df.iloc[row_idx].tolist()
            
            # Pad with empty strings if row has fewer columns than expected
            while len(row_data) < 8:
                row_data.append("")
                
            # Truncate if row has more columns than expected
            row_data = row_data[:8]
            
            # Set default values for required columns
            if len(row_data) < 6:
                row_data.extend([""] * (6 - len(row_data)))
            if len(row_data) < 7:
                row_data.append("")  # PartNumber
            if len(row_data) < 8:
                row_data.append("0")  # Done
            else:
                row_data[7] = "0"  # Ensure Done is 0
                
            all_data.append(row_data)
            
        self.log_message(f"  Added {len(rows_to_include)} rows")
        
    def calculate_part_numbers(self, all_data):
        """Calculate PartNumber based on TopicID and character limits"""
        self.log_message("Calculating PartNumbers...")
        
        # Group data by TopicID
        topic_groups = {}
        for row in all_data:
            topic_id = str(row[5]) if len(row) > 5 and pd.notna(row[5]) else ""
            if topic_id not in topic_groups:
                topic_groups[topic_id] = []
            topic_groups[topic_id].append(row)
            
        # Process each topic group
        for topic_id, rows in topic_groups.items():
            if not topic_id:
                continue
                
            part_number = 1
            char_count = 0
            
            for row in rows:
                text_content = str(row[4]) if len(row) > 4 and pd.notna(row[4]) else ""
                text_length = len(text_content)
                
                # Check if adding this text would exceed 4096 characters
                if char_count + text_length > 4096 and char_count > 0:
                    part_number += 1
                    char_count = text_length
                else:
                    char_count += text_length
                    
                # Set PartNumber
                if len(row) > 6:
                    row[6] = str(part_number)
                else:
                    row.extend([""] * (7 - len(row)))
                    row[6] = str(part_number)
                    
            self.log_message(f"  TopicID {topic_id}: {len(rows)} rows, {part_number} parts")
            
    def process_files(self):
        """Main processing function"""
        if not self.input_folder.get():
            messagebox.showerror("Error", "Please select an input folder")
            return
            
        if not os.path.exists(self.input_folder.get()):
            messagebox.showerror("Error", "Input folder does not exist")
            return
            
        try:
            self.progress.start()
            self.status_label.config(text="Processing files...")
            self.log_text.delete(1.0, tk.END)
            
            # Get all CSV files in the input folder
            csv_files = []
            for file in os.listdir(self.input_folder.get()):
                if file.lower().endswith('.csv'):
                    csv_files.append(os.path.join(self.input_folder.get(), file))
                    
            if not csv_files:
                messagebox.showwarning("Warning", "No CSV files found in the selected folder")
                return
                
            self.log_message(f"Found {len(csv_files)} CSV files")
            
            # Process all CSV files
            all_data = []
            for csv_file in csv_files:
                self.process_csv_file(csv_file, all_data)
                
            if not all_data:
                messagebox.showwarning("Warning", "No data found in CSV files")
                return
                
            self.log_message(f"Total rows collected: {len(all_data)}")
            
            # Calculate PartNumbers
            self.calculate_part_numbers(all_data)
            
            # Create output CSV
            output_path = os.path.join(self.input_folder.get(), self.output_file.get())
            
            # Define column headers
            headers = ['Chapter', 'Subchapter', 'Topic', 'Paragraph', 'Text', 'TopicID', 'PartNumber', 'Done']
            
            # Write to CSV with proper encoding for Farsi text
            with open(output_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.writer(csvfile, delimiter=';')
                writer.writerow(headers)
                writer.writerows(all_data)
                
            self.log_message(f"Output file created: {output_path}")
            self.log_message(f"Total rows written: {len(all_data)}")
            
            self.status_label.config(text="Processing completed successfully!")
            messagebox.showinfo("Success", f"Processing completed!\nOutput file: {output_path}\nTotal rows: {len(all_data)}")
            
        except Exception as e:
            self.log_message(f"Error: {str(e)}")
            messagebox.showerror("Error", f"An error occurred: {str(e)}")
            
        finally:
            self.progress.stop()
            self.status_label.config(text="Ready to process files")

def main():
    root = tk.Tk()
    app = TTSInputEditor(root)
    root.mainloop()

if __name__ == "__main__":
    main()
