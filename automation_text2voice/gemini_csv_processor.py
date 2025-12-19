import csv
import os
import sys
import logging
from datetime import datetime
import google.generativeai as genai
from typing import List, Dict, Optional
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import threading

class GeminiCSVProcessor:
    def __init__(self):
        self.setup_logging()
        self.api_keys = []
        self.current_api_key_index = 0
        self.model = None
        self.study_csv_data = []
        self.questions_csv_data = []
        self.prompt_template = ""
        self.output_file_data = []
        # Add file paths for sending to Gemini
        self.input_folder_path = ""
        self.output_file_csv_path = ""
        
    def find_csv_files(self, chapter_number: str) -> tuple:
        """Find CSV files based on chapter number pattern"""
        study_file = None
        questions_file = None
        
        if not self.input_folder_path or not os.path.exists(self.input_folder_path):
            return None, None
        
        # Look for files starting with 'a' + chapter_number
        study_pattern = f"a{chapter_number}.csv"
        questions_pattern = f"b{chapter_number}.csv"
        
        for filename in os.listdir(self.input_folder_path):
            if filename.lower() == study_pattern.lower():
                study_file = os.path.join(self.input_folder_path, filename)
            elif filename.lower() == questions_pattern.lower():
                questions_file = os.path.join(self.input_folder_path, filename)
        
        return study_file, questions_file
    
    def extract_chapter_and_topic_numbers(self, topic_id: str) -> tuple:
        """Extract chapter number (first 4 digits) and topic number (first 6 digits) from TopicID"""
        # Remove any non-digit characters and get the numeric part
        numeric_part = ''.join(filter(str.isdigit, topic_id))
        
        if len(numeric_part) >= 4:
            chapter_number = numeric_part[:4]
            topic_number = numeric_part[:6] if len(numeric_part) >= 6 else numeric_part
            self.logger.info(f"Extracted from TopicID '{topic_id}': chapter={chapter_number}, topic={topic_number}")
            return chapter_number, topic_number
        else:
            self.logger.warning(f"TopicID '{topic_id}' doesn't have enough digits for chapter/topic extraction (found: '{numeric_part}')")
            return None, None

    def setup_logging(self):
        """Setup logging configuration"""
        log_filename = f"gemini_processor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def load_api_keys(self, apikey_file_path: str) -> bool:
        """Load API keys from CSV file"""
        try:
            with open(apikey_file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file, delimiter=';')
                self.api_keys = []
                for row in reader:
                    if 'api_key' in row and row['api_key'].strip():
                        self.api_keys.append({
                            'account': row.get('account', 'Unknown'),
                            'project': row.get('project', 'Unknown'),
                            'api_key': row['api_key'].strip()
                        })
            
            if not self.api_keys:
                self.logger.error("No valid API keys found in the file")
                return False
                
            self.logger.info(f"Loaded {len(self.api_keys)} API keys")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading API keys: {str(e)}")
            return False
    
    def load_csv_file(self, file_path: str) -> List[Dict]:
        """Load CSV file and return as list of dictionaries"""
        try:
            with open(file_path, 'r', encoding='utf-8-sig') as file:  # utf-8-sig handles BOM
                # Try to detect delimiter
                sample = file.read(1024)
                file.seek(0)
                
                # Check for semicolon delimiter first
                if ';' in sample:
                    reader = csv.DictReader(file, delimiter=';')
                else:
                    reader = csv.DictReader(file, delimiter=',')
                
                data = list(reader)
                self.logger.info(f"Loaded {len(data)} rows from {file_path}")
                return data
                
        except Exception as e:
            self.logger.error(f"Error loading CSV file {file_path}: {str(e)}")
            return []
    
    def load_prompt_template(self, prompt_file_path: str) -> bool:
        """Load prompt template from text file"""
        try:
            with open(prompt_file_path, 'r', encoding='utf-8') as file:
                self.prompt_template = file.read()
            
            if not self.prompt_template.strip():
                self.logger.error("Prompt template file is empty")
                return False
                
            self.logger.info(f"Loaded prompt template from {prompt_file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading prompt template: {str(e)}")
            return False
    
    def get_next_api_key(self) -> Optional[Dict]:
        """Get next API key in rotation"""
        if not self.api_keys:
            return None
            
        api_key_data = self.api_keys[self.current_api_key_index]
        self.current_api_key_index = (self.current_api_key_index + 1) % len(self.api_keys)
        return api_key_data
    
    def setup_gemini_model(self, model_name: str, api_key: str):
        """Setup Gemini model with API key"""
        try:
            genai.configure(api_key=api_key)
            
            # Use correct model names from the documentation
            if model_name == "gemini-2.5-pro":
                self.model = genai.GenerativeModel('gemini-2.5-pro')
            elif model_name == "gemini-2.5-flash":
                self.model = genai.GenerativeModel('gemini-2.5-flash')
            else:
                self.model = genai.GenerativeModel('gemini-2.5-flash')  # Default to flash
                
            self.logger.info(f"Setup Gemini model: {model_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error setting up Gemini model: {str(e)}")
            return False
    
    def generate_response(self, prompt: str, api_key_data: Dict, chapter_number: str) -> Optional[str]:
        """Generate response from Gemini API with CSV files"""
        try:
            # Setup model with current API key
            if not self.setup_gemini_model(self.model_name, api_key_data['api_key']):
                return None
            
            # Find CSV files based on chapter number
            study_file_path, questions_file_path = self.find_csv_files(chapter_number)
            
            # Read CSV files and include their content in the prompt
            enhanced_prompt = prompt
            
            # Add study CSV content to prompt
            if study_file_path and os.path.exists(study_file_path):
                try:
                    with open(study_file_path, 'r', encoding='utf-8-sig') as file:
                        study_content = file.read()
                        enhanced_prompt += f"\n\nStudy CSV Content:\n{study_content}"
                        self.logger.info(f"Added study CSV content: {os.path.basename(study_file_path)}")
                except Exception as e:
                    self.logger.warning(f"Could not read study CSV file: {e}")
            else:
                self.logger.warning(f"Study CSV file not found for chapter {chapter_number}")
            
            # Add questions CSV content to prompt
            if questions_file_path and os.path.exists(questions_file_path):
                try:
                    with open(questions_file_path, 'r', encoding='utf-8-sig') as file:
                        questions_content = file.read()
                        enhanced_prompt += f"\n\nQuestions CSV Content:\n{questions_content}"
                        self.logger.info(f"Added questions CSV content: {os.path.basename(questions_file_path)}")
                except Exception as e:
                    self.logger.warning(f"Could not read questions CSV file: {e}")
            else:
                self.logger.warning(f"Questions CSV file not found for chapter {chapter_number}")
            
            # Add output file name CSV content to prompt
            if self.output_file_csv_path and os.path.exists(self.output_file_csv_path):
                try:
                    with open(self.output_file_csv_path, 'r', encoding='utf-8-sig') as file:
                        output_content = file.read()
                        enhanced_prompt += f"\n\nOutput File Structure:\n{output_content}"
                        self.logger.info(f"Added output file content: {os.path.basename(self.output_file_csv_path)}")
                except Exception as e:
                    self.logger.warning(f"Could not read output CSV file: {e}")
            
            # Generate response with enhanced prompt
            self.logger.info("Sending enhanced prompt with CSV content")
            response = self.model.generate_content(enhanced_prompt)
            
            if response and response.text:
                return response.text
            else:
                self.logger.error("Empty response from Gemini API")
                return None
                
        except Exception as e:
            self.logger.error(f"Error generating response: {str(e)}")
            return None
    
    def save_response_to_csv(self, topic_id: str, response: str, output_dir: str):
        """Save response to CSV file named after TopicID"""
        try:
            filename = f"{topic_id}.csv"
            filepath = os.path.join(output_dir, filename)
            
            # Clean the response - remove markdown formatting if present
            cleaned_response = response.strip()
            if cleaned_response.startswith('```csv'):
                cleaned_response = cleaned_response[7:]  # Remove ```csv
            if cleaned_response.endswith('```'):
                cleaned_response = cleaned_response[:-3]  # Remove ```
            cleaned_response = cleaned_response.strip()
            
            # Check if the response contains CSV-like structure with semicolons
            if ';' in cleaned_response and '\n' in cleaned_response:
                # It looks like CSV data with semicolon delimiter, save it directly with BOM for Excel compatibility
                with open(filepath, 'w', encoding='utf-8-sig', newline='') as file:
                    file.write(cleaned_response)
                self.logger.info(f"Saved CSV response to {filepath}")
            else:
                # It's plain text, save as structured CSV with semicolon delimiter and BOM for Excel compatibility
                with open(filepath, 'w', encoding='utf-8-sig', newline='') as file:
                    writer = csv.writer(file, delimiter=';')
                    writer.writerow(['فصل', 'زیرفصل', 'مبحث', 'شماره پاراگراف', 'متن پاراگراف', 'شماره مبحث', '0', '0'])
                    writer.writerow(['کلیات', 'تعریف', topic_id, '1', cleaned_response, f'a_{topic_id}', '0', '0'])
                self.logger.info(f"Saved text response to {filepath}")
            
        except Exception as e:
            self.logger.error(f"Error saving response to CSV: {str(e)}")
    
    def process_output_file(self, output_dir: str, progress_callback=None):
        """Process each row in the output file"""
        if not self.output_file_data:
            self.logger.error("No output file data loaded")
            return False
        
        total_rows = len(self.output_file_data)
        
        # Debug: Log the first row to see what columns are available
        if self.output_file_data:
            first_row = self.output_file_data[0]
            self.logger.info(f"Available columns in CSV: {list(first_row.keys())}")
            self.logger.info(f"First row data: {first_row}")
        
        for index, row in enumerate(self.output_file_data):
            try:
                # Check Done column - skip if already processed
                done_value = (row.get('Done', '') or 
                             row.get('done', '') or 
                             row.get('تکمیل', '') or  # Persian/Farsi for "done"
                             '')
                
                if str(done_value).strip() == '1':
                    self.logger.info(f"Row {index + 1}: Already processed (Done=1), skipping")
                    continue
                
                # Try different possible column names for Topic
                topic = (row.get('Topic', '') or 
                        row.get('topic', '') or 
                        row.get('\ufeffTopic', '') or  # Handle BOM character
                        row.get('مبحث', '') or  # Persian/Farsi for "topic"
                        '')
                
                # Try different possible column names for TopicID
                topic_id = (row.get('TopicID', '') or 
                           row.get('topicID', '') or 
                           row.get('topic_id', '') or 
                           row.get('ID', '') or 
                           row.get('id', '') or 
                           f'topic_{index}')
                
                # Try different possible column names for Chapter
                chapter = (row.get('Chapter', '') or 
                          row.get('chapter', '') or 
                          row.get('فصل', '') or  # Persian/Farsi for "chapter"
                          '')
                
                # Try different possible column names for Subchapter
                subchapter = (row.get('Subchapter', '') or 
                             row.get('subchapter', '') or 
                             row.get('زیرفصل', '') or  # Persian/Farsi for "subchapter"
                             '')
                
                if not topic:
                    self.logger.warning(f"Row {index + 1}: No Topic found, skipping. Available fields: {list(row.keys())}")
                    continue
                
                # Extract chapter and topic numbers from TopicID
                chapter_number, topic_number = self.extract_chapter_and_topic_numbers(topic_id)
                if not chapter_number:
                    self.logger.warning(f"Row {index + 1}: Could not extract chapter number from TopicID '{topic_id}', skipping")
                    continue
                
                # Get next API key
                api_key_data = self.get_next_api_key()
                if not api_key_data:
                    self.logger.error("No API key available")
                    return False
                
                # Create prompt by replacing ALL occurrences of placeholder
                prompt = self.prompt_template.replace('topicshouldreplacehere', topic)
                
                # Log request details
                self.logger.info(f"Processing row {index + 1}/{total_rows}")
                self.logger.info(f"API Key: {api_key_data['api_key'][:10]}...")
                self.logger.info(f"Account: {api_key_data['account']}")
                self.logger.info(f"Project: {api_key_data['project']}")
                self.logger.info(f"Chapter: {chapter}")
                self.logger.info(f"Subchapter: {subchapter}")
                self.logger.info(f"Topic: {topic}")
                
                # Generate response
                response = self.generate_response(prompt, api_key_data, chapter_number)
                
                if response:
                    self.logger.info("Request successful")
                    self.save_response_to_csv(topic_id, response, output_dir)
                    # Update Done column in the output file
                    self.update_done_column(index, self.output_file_csv_path)
                else:
                    self.logger.error("Request failed")
                
                # Update progress
                if progress_callback:
                    progress = (index + 1) / total_rows * 100
                    progress_callback(progress)
                
            except Exception as e:
                self.logger.error(f"Error processing row {index + 1}: {str(e)}")
                continue
        
        self.logger.info("Processing completed")
        return True
    
    def update_done_column(self, row_index: int, csv_file_path: str):
        """Update the Done column for a specific row in the CSV file"""
        try:
            # Read the CSV file
            with open(csv_file_path, 'r', encoding='utf-8-sig') as file:
                # Try to detect delimiter
                sample = file.read(1024)
                file.seek(0)
                
                # Check for semicolon delimiter first
                if ';' in sample:
                    reader = csv.DictReader(file, delimiter=';')
                    delimiter = ';'
                else:
                    reader = csv.DictReader(file, delimiter=',')
                    delimiter = ','
                
                rows = list(reader)
                fieldnames = reader.fieldnames
            
            # Update the Done column for the specific row
            if row_index < len(rows):
                # Find Done column name
                done_column = None
                for col in ['Done', 'done', 'تکمیل']:
                    if col in fieldnames:
                        done_column = col
                        break
                
                if done_column:
                    rows[row_index][done_column] = '1'
                    self.logger.info(f"Updated Done column for row {row_index + 1}")
                else:
                    # Add Done column if it doesn't exist
                    done_column = 'Done'
                    for row in rows:
                        row[done_column] = '0'
                    rows[row_index][done_column] = '1'
                    fieldnames.append(done_column)
                    self.logger.info(f"Added Done column and updated for row {row_index + 1}")
                
                # Write back to CSV file
                with open(csv_file_path, 'w', encoding='utf-8-sig', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter=delimiter)
                    writer.writeheader()
                    writer.writerows(rows)
                
                self.logger.info(f"Successfully updated CSV file: {csv_file_path}")
            else:
                self.logger.warning(f"Row index {row_index} out of range")
                
        except Exception as e:
            self.logger.error(f"Error updating Done column: {str(e)}")

class GeminiProcessorGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Gemini CSV Processor")
        self.root.geometry("600x500")
        
        self.processor = GeminiCSVProcessor()
        self.setup_ui()
        
    def setup_ui(self):
        """Setup the user interface"""
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # File selection section
        ttk.Label(main_frame, text="Input Files", font=('Arial', 12, 'bold')).grid(row=0, column=0, columnspan=2, pady=(0, 10))
        
        # Input folder
        ttk.Label(main_frame, text="Input Folder:").grid(row=1, column=0, sticky=tk.W, pady=2)
        self.input_folder_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.input_folder_var, width=50).grid(row=1, column=1, padx=(5, 5), pady=2)
        ttk.Button(main_frame, text="Browse", command=self.browse_input_folder).grid(row=1, column=2, pady=2)
        
        # Info label
        ttk.Label(main_frame, text="Note: App will auto-find CSV files with pattern 'a[chapter].csv' and 'b[chapter].csv'", 
                 font=('Arial', 8, 'italic')).grid(row=2, column=0, columnspan=3, pady=2)
        
        # Prompt file
        ttk.Label(main_frame, text="Prompt TXT File:").grid(row=3, column=0, sticky=tk.W, pady=2)
        self.prompt_file_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.prompt_file_var, width=50).grid(row=3, column=1, padx=(5, 5), pady=2)
        ttk.Button(main_frame, text="Browse", command=self.browse_prompt_file).grid(row=3, column=2, pady=2)
        
        # Output file name CSV
        ttk.Label(main_frame, text="Output File Name CSV:").grid(row=4, column=0, sticky=tk.W, pady=2)
        self.output_file_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.output_file_var, width=50).grid(row=4, column=1, padx=(5, 5), pady=2)
        ttk.Button(main_frame, text="Browse", command=self.browse_output_file).grid(row=4, column=2, pady=2)
        
        # API Key list CSV
        ttk.Label(main_frame, text="API Key List CSV:").grid(row=5, column=0, sticky=tk.W, pady=2)
        self.apikey_file_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.apikey_file_var, width=50).grid(row=5, column=1, padx=(5, 5), pady=2)
        ttk.Button(main_frame, text="Browse", command=self.browse_apikey_file).grid(row=5, column=2, pady=2)
        
        # Model selection
        ttk.Label(main_frame, text="Gemini Model:").grid(row=6, column=0, sticky=tk.W, pady=2)
        self.model_var = tk.StringVar(value="gemini-2.5-flash")
        model_combo = ttk.Combobox(main_frame, textvariable=self.model_var, values=["gemini-2.5-pro", "gemini-2.5-flash"], state="readonly")
        model_combo.grid(row=6, column=1, sticky=tk.W, padx=(5, 5), pady=2)
        
        # Output directory
        ttk.Label(main_frame, text="Output Directory:").grid(row=7, column=0, sticky=tk.W, pady=2)
        self.output_dir_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.output_dir_var, width=50).grid(row=7, column=1, padx=(5, 5), pady=2)
        ttk.Button(main_frame, text="Browse", command=self.browse_output_dir).grid(row=7, column=2, pady=2)
        
        # Progress bar
        ttk.Label(main_frame, text="Progress:").grid(row=8, column=0, sticky=tk.W, pady=(10, 2))
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(main_frame, variable=self.progress_var, maximum=100)
        self.progress_bar.grid(row=8, column=1, columnspan=2, sticky=(tk.W, tk.E), padx=(5, 5), pady=(10, 2))
        
        # Process button
        self.process_button = ttk.Button(main_frame, text="Start Processing", command=self.start_processing)
        self.process_button.grid(row=9, column=0, columnspan=3, pady=20)
        
        # Status label
        self.status_var = tk.StringVar(value="Ready")
        ttk.Label(main_frame, textvariable=self.status_var).grid(row=10, column=0, columnspan=3)
        
    def browse_input_folder(self):
        dirname = filedialog.askdirectory(title="Select Input Folder")
        if dirname:
            self.input_folder_var.set(dirname)
    
    def browse_prompt_file(self):
        filename = filedialog.askopenfilename(title="Select Prompt TXT File", filetypes=[("Text files", "*.txt")])
        if filename:
            self.prompt_file_var.set(filename)
    
    def browse_output_file(self):
        filename = filedialog.askopenfilename(title="Select Output File Name CSV", filetypes=[("CSV files", "*.csv")])
        if filename:
            self.output_file_var.set(filename)
    
    def browse_apikey_file(self):
        filename = filedialog.askopenfilename(title="Select API Key List CSV", filetypes=[("CSV files", "*.csv")])
        if filename:
            self.apikey_file_var.set(filename)
    
    def browse_output_dir(self):
        dirname = filedialog.askdirectory(title="Select Output Directory")
        if dirname:
            self.output_dir_var.set(dirname)
    
    def validate_inputs(self) -> bool:
        """Validate all input files and settings"""
        required_files = [
            ("Input Folder", self.input_folder_var.get()),
            ("Prompt TXT", self.prompt_file_var.get()),
            ("Output File Name CSV", self.output_file_var.get()),
            ("API Key List CSV", self.apikey_file_var.get())
        ]
        
        for name, path in required_files:
            if not path:
                messagebox.showerror("Error", f"Please select {name}")
                return False
            if not os.path.exists(path):
                messagebox.showerror("Error", f"{name} does not exist: {path}")
                return False
        
        if not self.output_dir_var.get():
            messagebox.showerror("Error", "Please select output directory")
            return False
        
        return True
    
    def load_files(self) -> bool:
        """Load all input files"""
        try:
            # Load API keys
            if not self.processor.load_api_keys(self.apikey_file_var.get()):
                messagebox.showerror("Error", "Failed to load API keys")
                return False
            
            # Set input folder path
            self.processor.input_folder_path = self.input_folder_var.get()
            
            # Load output file data
            self.processor.output_file_data = self.processor.load_csv_file(self.output_file_var.get())
            
            # Store output file path for sending to Gemini
            self.processor.output_file_csv_path = self.output_file_var.get()
            
            # Load prompt template
            if not self.processor.load_prompt_template(self.prompt_file_var.get()):
                messagebox.showerror("Error", "Failed to load prompt template")
                return False
            
            # Set model name
            self.processor.model_name = self.model_var.get()
            
            return True
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load files: {str(e)}")
            return False
    
    def update_progress(self, progress):
        """Update progress bar"""
        self.progress_var.set(progress)
        self.root.update_idletasks()
    
    def start_processing(self):
        """Start the processing in a separate thread"""
        if not self.validate_inputs():
            return
        
        if not self.load_files():
            return
        
        # Disable process button
        self.process_button.config(state="disabled")
        self.status_var.set("Processing...")
        
        # Start processing in separate thread
        thread = threading.Thread(target=self.process_files)
        thread.daemon = True
        thread.start()
    
    def process_files(self):
        """Process files in background thread"""
        try:
            success = self.processor.process_output_file(
                self.output_dir_var.get(),
                self.update_progress
            )
            
            if success:
                self.status_var.set("Processing completed successfully!")
                messagebox.showinfo("Success", "Processing completed successfully!")
            else:
                self.status_var.set("Processing failed")
                messagebox.showerror("Error", "Processing failed. Check the log file for details.")
                
        except Exception as e:
            self.status_var.set("Processing failed")
            messagebox.showerror("Error", f"Processing failed: {str(e)}")
        
        finally:
            # Re-enable process button
            self.process_button.config(state="normal")
    
    def run(self):
        """Run the GUI application"""
        self.root.mainloop()

def main():
    """Main function"""
    app = GeminiProcessorGUI()
    app.run()

if __name__ == "__main__":
    main()
