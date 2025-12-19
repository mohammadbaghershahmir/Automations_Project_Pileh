import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import os
import pandas as pd
from pathlib import Path
import threading

class ExcelToCsvConverter:
    def __init__(self, root):
        self.root = root
        self.root.title("Excel to CSV Converter (UTF-8 BOM)")
        self.root.geometry("600x500")
        self.folder_path = None
        self.output_folder_path = None
        
        # Create GUI elements
        self.create_widgets()
        
    def create_widgets(self):
        # Main frame
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Title label
        title_label = ttk.Label(main_frame, text="Excel to CSV Converter", font=("Arial", 16, "bold"))
        title_label.grid(row=0, column=0, columnspan=3, pady=(0, 20))
        
        # Input folder selection
        ttk.Label(main_frame, text="Input Folder:", font=("Arial", 10, "bold")).grid(row=1, column=0, sticky=tk.W, pady=(10, 5))
        
        self.folder_label = ttk.Label(main_frame, text="No folder selected", foreground="gray", wraplength=400)
        self.folder_label.grid(row=2, column=0, columnspan=2, sticky=tk.W, pady=5)
        
        browse_button = ttk.Button(main_frame, text="Browse Input", command=self.browse_folder)
        browse_button.grid(row=1, column=1, pady=5, padx=10)
        
        # Output folder selection
        ttk.Label(main_frame, text="Output Folder:", font=("Arial", 10, "bold")).grid(row=3, column=0, sticky=tk.W, pady=(15, 5))
        
        self.output_folder_label = ttk.Label(main_frame, text="No folder selected", foreground="gray", wraplength=400)
        self.output_folder_label.grid(row=4, column=0, columnspan=2, sticky=tk.W, pady=5)
        
        browse_output_button = ttk.Button(main_frame, text="Browse Output", command=self.browse_output_folder)
        browse_output_button.grid(row=3, column=1, pady=5, padx=10)
        
        # Convert button
        self.convert_button = ttk.Button(main_frame, text="Convert Excel to CSV", command=self.convert_files, state="disabled")
        self.convert_button.grid(row=5, column=0, columnspan=3, pady=20)
        
        # Progress bar
        self.progress = ttk.Progressbar(main_frame, mode='indeterminate')
        self.progress.grid(row=6, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=10)
        
        # Status text
        self.status_text = tk.Text(main_frame, height=8, wrap=tk.WORD, state="disabled")
        self.status_text.grid(row=7, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S), pady=10)
        
        # Scrollbar for status text
        scrollbar = ttk.Scrollbar(main_frame, orient="vertical", command=self.status_text.yview)
        scrollbar.grid(row=7, column=3, sticky=(tk.N, tk.S))
        self.status_text.configure(yscrollcommand=scrollbar.set)
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(2, weight=1)
        main_frame.rowconfigure(7, weight=1)
        
    def browse_folder(self):
        folder = filedialog.askdirectory(title="Select folder containing Excel files")
        if folder:
            self.folder_path = folder
            self.folder_label.config(text=f"📁 {folder}", foreground="black")
            self.check_ready()
            self.log_message(f"Selected input folder: {folder}\n")
    
    def browse_output_folder(self):
        folder = filedialog.askdirectory(title="Select output folder for CSV files")
        if folder:
            self.output_folder_path = folder
            self.output_folder_label.config(text=f"📁 {folder}", foreground="black")
            self.check_ready()
            self.log_message(f"Selected output folder: {folder}\n")
    
    def check_ready(self):
        if self.folder_path and self.output_folder_path:
            self.convert_button.config(state="normal")
            
    def log_message(self, message):
        self.status_text.config(state="normal")
        self.status_text.insert(tk.END, message)
        self.status_text.see(tk.END)
        self.status_text.config(state="disabled")
        self.root.update()
        
    def convert_excel_to_csv(self):
        if not self.folder_path or not self.output_folder_path:
            messagebox.showerror("Error", "Please select both input and output folders!")
            return
            
        # Find all Excel files
        excel_files = []
        for ext in ['*.xlsx', '*.xls']:
            excel_files.extend(Path(self.folder_path).rglob(ext))
        
        if not excel_files:
            messagebox.showwarning("Warning", "No Excel files found in the selected folder!")
            self.progress.stop()
            return
        
        self.log_message(f"Found {len(excel_files)} Excel file(s) to convert.\n")
        self.log_message("Starting conversion...\n\n")
        
        # Create output directory if it doesn't exist
        Path(self.output_folder_path).mkdir(parents=True, exist_ok=True)
        
        converted_count = 0
        error_count = 0
        
        for excel_file in excel_files:
            try:
                # Read Excel file
                self.log_message(f"Reading: {excel_file.name}\n")
                df = pd.read_excel(excel_file)
                
                # Create CSV path in output folder
                csv_filename = excel_file.stem + '.csv'
                csv_path = Path(self.output_folder_path) / csv_filename
                
                # Save as CSV with UTF-8-SIG encoding (includes BOM for Excel compatibility)
                df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                
                self.log_message(f"✓ Converted: {csv_filename}\n")
                converted_count += 1
                
            except Exception as e:
                self.log_message(f"✗ Error converting {excel_file.name}: {str(e)}\n")
                error_count += 1
        
        self.progress.stop()
        self.log_message(f"\n{'='*50}\n")
        self.log_message(f"Conversion complete!\n")
        self.log_message(f"✓ Successfully converted: {converted_count}\n")
        self.log_message(f"✗ Errors: {error_count}\n")
        
        messagebox.showinfo("Conversion Complete", 
                           f"Successfully converted {converted_count} file(s).\n"
                           f"Encountered {error_count} error(s).")
        
        # Disable convert button after completion
        self.convert_button.config(state="disabled")
        
    def convert_files(self):
        self.status_text.config(state="normal")
        self.status_text.delete(1.0, tk.END)
        self.status_text.config(state="disabled")
        
        # Disable button and start progress
        self.convert_button.config(state="disabled")
        self.progress.start()
        
        # Run conversion in a separate thread to prevent GUI freezing
        thread = threading.Thread(target=self.convert_excel_to_csv)
        thread.daemon = True
        thread.start()

def main():
    root = tk.Tk()
    app = ExcelToCsvConverter(root)
    root.mainloop()

if __name__ == "__main__":
    main()

