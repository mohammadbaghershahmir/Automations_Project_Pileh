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

class GeminiQuestionsProcessor:
    def __init__(self):
        self.setup_logging()
        self.api_keys = []
        self.current_api_key_index = 0
        self.model = None
        self.prompt_template = ""
        self.questions_data = []
        self.study_material_folder = ""
        self.questions_csv_path = ""
        self.model_name = ""
        self.output_csv_path = ""
        
    def setup_logging(self):
        """Setup logging configuration"""
        log_filename = f"gemini_questions_processor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
            with open(file_path, 'r', encoding='utf-8-sig') as file:
                sample = file.read(1024)
                file.seek(0)
                
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
    
    def find_study_material_file(self, chapter_name: str) -> Optional[str]:
        """Find study material file based on chapter name"""
        try:
            if not os.path.exists(self.study_material_folder):
                return None
            
            # Normalize chapter name for matching
            chapter_normalized = chapter_name.strip()
            
            # Look for files starting with 'chapter_'
            for filename in os.listdir(self.study_material_folder):
                if filename.lower().startswith('chapter_'):
                    # Extract chapter name from filename (after chapter_xx_)
                    parts = filename.replace('chapter_', '').split('_', 1)
                    if len(parts) > 1:
                        file_chapter_name = parts[1].rsplit('.', 1)[0]  # Remove extension
                        
                        # Check if chapter name matches
                        if chapter_normalized in file_chapter_name or file_chapter_name in chapter_normalized:
                            file_path = os.path.join(self.study_material_folder, filename)
                            self.logger.info(f"Found matching study material: {filename} for chapter: {chapter_name}")
                            return file_path
            
            self.logger.warning(f"No matching study material found for chapter: {chapter_name}")
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding study material for chapter '{chapter_name}': {str(e)}")
            return None
    
    def get_unique_topics(self) -> List[Dict]:
        """Group questions by unique Topic (Chapter, Subchapter, Topic)"""
        topics_dict = {}
        
        for row in self.questions_data:
            # Get Topic, Chapter, Subchapter
            topic = (row.get('Topic', '') or row.get('topic', '') or row.get('مبحث', '')).strip()
            chapter = (row.get('Chapter', '') or row.get('chapter', '') or row.get('فصل', '')).strip()
            subchapter = (row.get('Subchapter', '') or row.get('subchapter', '') or row.get('زیرفصل', '')).strip()
            
            if not topic:
                continue
            
            # Create unique key
            unique_key = f"{chapter}_{subchapter}_{topic}"
            
            if unique_key not in topics_dict:
                topics_dict[unique_key] = {
                    'topic': topic,
                    'chapter': chapter,
                    'subchapter': subchapter,
                    'rows': []
                }
            
            topics_dict[unique_key]['rows'].append(row)
        
        unique_topics = list(topics_dict.values())
        self.logger.info(f"Found {len(unique_topics)} unique topics")
        return unique_topics
    
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
            
            if model_name == "gemini-2.5-pro":
                self.model = genai.GenerativeModel('gemini-2.5-pro')
            elif model_name == "gemini-2.5-flash":
                self.model = genai.GenerativeModel('gemini-2.5-flash')
            else:
                self.model = genai.GenerativeModel('gemini-2.5-flash')
                
            self.logger.info(f"Setup Gemini model: {model_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error setting up Gemini model: {str(e)}")
            return False
    
    def generate_response(self, prompt: str, api_key_data: Dict, study_file_path: Optional[str], questions_rows: List[Dict]) -> Optional[str]:
        """Generate response from Gemini API"""
        try:
            if not self.setup_gemini_model(self.model_name, api_key_data['api_key']):
                return None
            
            enhanced_prompt = prompt
            
            # Add study material content
            if study_file_path and os.path.exists(study_file_path):
                try:
                    with open(study_file_path, 'r', encoding='utf-8-sig') as file:
                        study_content = file.read()
                        enhanced_prompt += f"\n\nStudy Material:\n{study_content}"
                        self.logger.info(f"Added study material: {os.path.basename(study_file_path)}")
                except Exception as e:
                    self.logger.warning(f"Could not read study material file: {e}")
            
            # Add questions content
            questions_text = ""
            for q in questions_rows:
                # Get all fields as key-value pairs
                row_text = "; ".join([f"{k}: {v}" for k, v in q.items()])
                questions_text += row_text + "\n"
            
            if questions_text:
                enhanced_prompt += f"\n\nQuestions:\n{questions_text}"
                self.logger.info(f"Added {len(questions_rows)} questions")
            
            # Generate response
            self.logger.info("Sending request to Gemini API")
            response = self.model.generate_content(enhanced_prompt)
            
            if response and response.text:
                return response.text
            else:
                self.logger.error("Empty response from Gemini API")
                return None
                
        except Exception as e:
            self.logger.error(f"Error generating response: {str(e)}")
            return None
    
    def is_topic_done(self, topic_info: Dict) -> bool:
        """Check if topic is already completed (Done=1)"""
        for row in topic_info['rows']:
            done_value = (row.get('Done', '') or row.get('done', '') or row.get('تکمیل', '') or '').strip()
            if str(done_value) == '1':
                return True
        return False
    
    def mark_topic_done(self, topic_info: Dict, questions_csv_path: str):
        """Mark all rows for this topic as Done=1"""
        try:
            # Read CSV file
            with open(questions_csv_path, 'r', encoding='utf-8-sig') as file:
                sample = file.read(1024)
                file.seek(0)
                
                if ';' in sample:
                    reader = csv.DictReader(file, delimiter=';')
                    delimiter = ';'
                else:
                    reader = csv.DictReader(file, delimiter=',')
                    delimiter = ','
                
                rows = list(reader)
                fieldnames = list(reader.fieldnames)
            
            # Find Done column
            done_column = None
            for col in ['Done', 'done', 'تکمیل']:
                if col in fieldnames:
                    done_column = col
                    break
            
            if not done_column:
                done_column = 'Done'
                for row in rows:
                    row[done_column] = '0'
                fieldnames.append(done_column)
            
            # Find and mark all rows for this topic
            topic = topic_info['topic']
            chapter = topic_info['chapter']
            subchapter = topic_info['subchapter']
            
            marked_count = 0
            for row in rows:
                row_topic = (row.get('Topic', '') or row.get('topic', '') or row.get('مبحث', '')).strip()
                row_chapter = (row.get('Chapter', '') or row.get('chapter', '') or row.get('فصل', '')).strip()
                row_subchapter = (row.get('Subchapter', '') or row.get('subchapter', '') or row.get('زیرفصل', '')).strip()
                
                if row_topic == topic and row_chapter == chapter and row_subchapter == subchapter:
                    row[done_column] = '1'
                    marked_count += 1
            
            # Write back to CSV
            with open(questions_csv_path, 'w', encoding='utf-8-sig', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter=delimiter)
                writer.writeheader()
                writer.writerows(rows)
            
            self.logger.info(f"Marked {marked_count} rows as Done=1 for topic: {topic}")
            
        except Exception as e:
            self.logger.error(f"Error marking topic as done: {str(e)}")
    
    def parse_csv_response(self, response_text: str) -> List[List[str]]:
        """Parse CSV response from AI into rows"""
        try:
            # Clean the response
            cleaned = response_text.strip()
            
            # Remove markdown formatting
            if cleaned.startswith('```csv'):
                cleaned = cleaned[7:]
            if cleaned.startswith('```'):
                cleaned = cleaned[3:]
            if cleaned.endswith('```'):
                cleaned = cleaned[:-3]
            cleaned = cleaned.strip()
            
            # Parse CSV
            rows = []
            lines = cleaned.split('\n')
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                # Support both semicolon and comma delimiters
                if ';' in line:
                    row = [cell.strip() for cell in line.split(';')]
                else:
                    row = [cell.strip() for cell in line.split(',')]
                
                rows.append(row)
            
            return rows
            
        except Exception as e:
            self.logger.error(f"Error parsing CSV response: {str(e)}")
            return []
    
    def process_all_topics(self, progress_callback=None):
        """Process all unique topics"""
        unique_topics = self.get_unique_topics()
        self.first_write = True  # Track if this is the first write to file
        self.output_csv_created = False
        
        total_topics = len(unique_topics)
        
        for index, topic_info in enumerate(unique_topics):
            try:
                # Check if already done
                if self.is_topic_done(topic_info):
                    self.logger.warning(f"Topic '{topic_info['topic']}' is already done, skipping")
                    continue
                
                topic = topic_info['topic']
                chapter = topic_info['chapter']
                self.logger.info(f"Processing topic {index + 1}/{total_topics}: {topic}")
                
                # Find study material file based on chapter name
                study_file_path = self.find_study_material_file(chapter)
                
                # Create prompt by replacing placeholder
                prompt = self.prompt_template.replace('topicshouldreplacehere', topic)
                
                # Get API key
                api_key_data = self.get_next_api_key()
                if not api_key_data:
                    self.logger.error("No API key available")
                    continue
                
                self.logger.info(f"Using API key: {api_key_data['account']} - {api_key_data['project']}")
                
                # Generate response
                questions_rows = topic_info['rows']
                response = self.generate_response(prompt, api_key_data, study_file_path, questions_rows)
                
                if response:
                    self.logger.info("Request successful")
                    
                    # Parse CSV response
                    csv_rows = self.parse_csv_response(response)
                    
                    if csv_rows:
                        # Append rows to output file immediately
                        self.append_to_csv_file(csv_rows)
                        self.logger.info(f"Added {len(csv_rows)} rows to output")
                    else:
                        self.logger.warning("Failed to parse CSV from response")
                    
                    # Mark topic as done
                    self.mark_topic_done(topic_info, self.questions_csv_path)
                else:
                    self.logger.error("Request failed")
                
                # Update progress
                if progress_callback:
                    progress = ((index + 1) / total_topics) * 100
                    progress_callback(progress)
                
            except Exception as e:
                self.logger.error(f"Error processing topic '{topic_info['topic']}': {str(e)}")
                continue
        
        self.logger.info("Processing completed")
        return True
    
    def append_to_csv_file(self, rows: List[List[str]]):
        """Append rows to the CSV file, creating it if it doesn't exist"""
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.output_csv_path), exist_ok=True)
            
            if self.first_write:
                # First write - create new file with all rows (including header if present)
                with open(self.output_csv_path, 'w', encoding='utf-8-sig', newline='') as file:
                    writer = csv.writer(file, delimiter=';')
                    for row in rows:
                        writer.writerow(row)
                self.logger.info(f"Created output CSV file with {len(rows)} rows: {self.output_csv_path}")
                self.first_write = False
            else:
                # Append mode - skip header row (first row) if it looks like a CSV header
                # Check if first row contains header-like text (non-numeric in first few columns)
                skip_first = False
                if len(rows) > 1:
                    first_row = rows[0]
                    # Heuristic: if first column contains text (not numeric), likely a header
                    skip_first = len(first_row) > 0 and not first_row[0].strip().replace('-', '').replace('.', '').isdigit()
                
                rows_to_write = rows[1:] if skip_first else rows
                with open(self.output_csv_path, 'a', encoding='utf-8-sig', newline='') as file:
                    writer = csv.writer(file, delimiter=';')
                    for row in rows_to_write:
                        writer.writerow(row)
                self.logger.info(f"Appended {len(rows_to_write)} rows to: {self.output_csv_path}")
            
        except Exception as e:
            self.logger.error(f"Error appending to CSV: {str(e)}")


class GeminiQuestionsGUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Gemini Questions Processor")
        self.root.geometry("650x580")
        
        self.processor = GeminiQuestionsProcessor()
        self.setup_ui()
        
    def setup_ui(self):
        """Setup the user interface"""
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Title
        ttk.Label(main_frame, text="Gemini Questions Processor", font=('Arial', 14, 'bold')).grid(row=0, column=0, columnspan=3, pady=(0, 15))
        
        # Study material folder
        ttk.Label(main_frame, text="Study Material Folder:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.study_folder_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.study_folder_var, width=50).grid(row=1, column=1, padx=5, pady=5)
        ttk.Button(main_frame, text="Browse", command=self.browse_study_folder).grid(row=1, column=2, pady=5)
        
        # Questions CSV
        ttk.Label(main_frame, text="Questions CSV File:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.questions_csv_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.questions_csv_var, width=50).grid(row=2, column=1, padx=5, pady=5)
        ttk.Button(main_frame, text="Browse", command=self.browse_questions_csv).grid(row=2, column=2, pady=5)
        
        # Prompt file
        ttk.Label(main_frame, text="Prompt TXT File:").grid(row=3, column=0, sticky=tk.W, pady=5)
        self.prompt_file_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.prompt_file_var, width=50).grid(row=3, column=1, padx=5, pady=5)
        ttk.Button(main_frame, text="Browse", command=self.browse_prompt_file).grid(row=3, column=2, pady=5)
        
        # API Key list CSV
        ttk.Label(main_frame, text="API Key List CSV:").grid(row=4, column=0, sticky=tk.W, pady=5)
        self.apikey_file_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.apikey_file_var, width=50).grid(row=4, column=1, padx=5, pady=5)
        ttk.Button(main_frame, text="Browse", command=self.browse_apikey_file).grid(row=4, column=2, pady=5)
        
        # Model selection
        ttk.Label(main_frame, text="Gemini Model:").grid(row=5, column=0, sticky=tk.W, pady=5)
        self.model_var = tk.StringVar(value="gemini-2.5-flash")
        model_combo = ttk.Combobox(main_frame, textvariable=self.model_var, values=["gemini-2.5-pro", "gemini-2.5-flash"], state="readonly")
        model_combo.grid(row=5, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Output CSV
        ttk.Label(main_frame, text="Output CSV File (Optional):").grid(row=6, column=0, sticky=tk.W, pady=5)
        self.output_csv_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.output_csv_var, width=50).grid(row=6, column=1, padx=5, pady=5)
        ttk.Button(main_frame, text="Browse", command=self.browse_output_csv).grid(row=6, column=2, pady=5)
        
        # Note about default output
        ttk.Label(main_frame, text="(Leave empty to create 'initial_output.csv' in the questions folder)", 
                 font=('Arial', 8, 'italic')).grid(row=7, column=1, columnspan=2, sticky=tk.W, pady=(0, 5))
        
        # Progress bar
        ttk.Label(main_frame, text="Progress:").grid(row=8, column=0, sticky=tk.W, pady=(15, 5))
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(main_frame, variable=self.progress_var, maximum=100)
        self.progress_bar.grid(row=8, column=1, columnspan=2, sticky=(tk.W, tk.E), padx=5, pady=(15, 5))
        
        # Process button
        self.process_button = ttk.Button(main_frame, text="Start Processing", command=self.start_processing)
        self.process_button.grid(row=9, column=0, columnspan=3, pady=20)
        
        # Status label
        self.status_var = tk.StringVar(value="Ready")
        ttk.Label(main_frame, textvariable=self.status_var).grid(row=10, column=0, columnspan=3)
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
    
    def browse_study_folder(self):
        dirname = filedialog.askdirectory(title="Select Study Material Folder")
        if dirname:
            self.study_folder_var.set(dirname)
    
    def browse_questions_csv(self):
        filename = filedialog.askopenfilename(title="Select Questions CSV", filetypes=[("CSV files", "*.csv")])
        if filename:
            self.questions_csv_var.set(filename)
    
    def browse_prompt_file(self):
        filename = filedialog.askopenfilename(title="Select Prompt TXT File", filetypes=[("Text files", "*.txt")])
        if filename:
            self.prompt_file_var.set(filename)
    
    def browse_apikey_file(self):
        filename = filedialog.askopenfilename(title="Select API Key List CSV", filetypes=[("CSV files", "*.csv")])
        if filename:
            self.apikey_file_var.set(filename)
    
    def browse_output_csv(self):
        filename = filedialog.asksaveasfilename(title="Save Output CSV As", defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
        if filename:
            self.output_csv_var.set(filename)
    
    def validate_inputs(self) -> bool:
        """Validate all inputs"""
        if not self.study_folder_var.get() or not os.path.exists(self.study_folder_var.get()):
            messagebox.showerror("Error", "Please select a valid study material folder")
            return False
        
        if not self.questions_csv_var.get() or not os.path.exists(self.questions_csv_var.get()):
            messagebox.showerror("Error", "Please select a valid questions CSV file")
            return False
        
        if not self.prompt_file_var.get() or not os.path.exists(self.prompt_file_var.get()):
            messagebox.showerror("Error", "Please select a valid prompt file")
            return False
        
        if not self.apikey_file_var.get() or not os.path.exists(self.apikey_file_var.get()):
            messagebox.showerror("Error", "Please select a valid API key CSV file")
            return False
        
        return True
    
    def load_files(self) -> bool:
        """Load all input files"""
        try:
            # Load API keys
            if not self.processor.load_api_keys(self.apikey_file_var.get()):
                messagebox.showerror("Error", "Failed to load API keys")
                return False
            
            # Set paths
            self.processor.study_material_folder = self.study_folder_var.get()
            self.processor.questions_csv_path = self.questions_csv_var.get()
            self.processor.model_name = self.model_var.get()
            
            # Create default output CSV path if not specified
            if self.output_csv_var.get():
                self.processor.output_csv_path = self.output_csv_var.get()
            else:
                # Create default path in same directory as questions CSV
                questions_dir = os.path.dirname(self.processor.questions_csv_path)
                default_filename = "initial_output.csv"
                self.processor.output_csv_path = os.path.join(questions_dir, default_filename)
                self.processor.logger.info(f"Using default output CSV path: {self.processor.output_csv_path}")
            
            # Load questions CSV
            self.processor.questions_data = self.processor.load_csv_file(self.processor.questions_csv_path)
            if not self.processor.questions_data:
                messagebox.showerror("Error", "Failed to load questions CSV")
                return False
            
            # Load prompt template
            if not self.processor.load_prompt_template(self.prompt_file_var.get()):
                messagebox.showerror("Error", "Failed to load prompt template")
                return False
            
            return True
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load files: {str(e)}")
            return False
    
    def update_progress(self, progress):
        """Update progress bar"""
        self.progress_var.set(progress)
        self.root.update_idletasks()
    
    def start_processing(self):
        """Start processing in background thread"""
        if not self.validate_inputs():
            return
        
        if not self.load_files():
            return
        
        self.process_button.config(state="disabled")
        self.status_var.set("Processing...")
        
        thread = threading.Thread(target=self.process_all)
        thread.daemon = True
        thread.start()
    
    def process_all(self):
        """Process all topics in background"""
        try:
            success = self.processor.process_all_topics(self.update_progress)
            
            if success:
                self.status_var.set("Processing completed successfully!")
                messagebox.showinfo("Success", "Processing completed successfully!")
            else:
                self.status_var.set("Processing failed")
                messagebox.showerror("Error", "Processing failed. Check log file for details.")
                
        except Exception as e:
            self.status_var.set("Processing failed")
            messagebox.showerror("Error", f"Processing failed: {str(e)}")
        
        finally:
            self.process_button.config(state="normal")
    
    def run(self):
        """Run the GUI"""
        self.root.mainloop()


def main():
    """Main function"""
    app = GeminiQuestionsGUI()
    app.run()


if __name__ == "__main__":
    main()
