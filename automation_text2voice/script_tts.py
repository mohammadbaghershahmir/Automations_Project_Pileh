import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import pandas as pd
import csv
import os
import asyncio
import wave
import threading
from pathlib import Path
import logging
from datetime import datetime

class ScriptTTSProcessor:
    def __init__(self):
        self.setup_logging()
        self.api_keys = []
        self.current_api_key_index = 0
        self.csv_data = []
        
    def setup_logging(self):
        """Setup logging configuration"""
        log_filename = f"script_tts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
                logging.StreamHandler()
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
    
    def load_csv_file(self, file_path: str) -> bool:
        """Load CSV file and return success status"""
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
                
                self.csv_data = list(reader)
                self.logger.info(f"Loaded {len(self.csv_data)} rows from {file_path}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error loading CSV file {file_path}: {str(e)}")
            return False
    
    def get_next_api_key(self) -> str:
        """Get next API key in rotation"""
        if not self.api_keys:
            return None
            
        api_key = self.api_keys[self.current_api_key_index]['api_key']
        self.current_api_key_index = (self.current_api_key_index + 1) % len(self.api_keys)
        return api_key
    
    def group_data_by_topic_and_part(self):
        """Group CSV data by TopicID and PartNumber"""
        groups = {}
        
        for row in self.csv_data:
            topic_id = str(row.get('TopicID', '')).strip()
            part_number = str(row.get('PartNumber', '')).strip()
            paragraph = str(row.get('Paragraph', '')).strip()
            text = str(row.get('Text', '')).strip()
            
            if not topic_id or not part_number:
                continue
                
            key = f"{topic_id}_{part_number}"
            if key not in groups:
                groups[key] = {
                    'topic_id': topic_id,
                    'part_number': part_number,
                    'paragraphs': []
                }
            
            groups[key]['paragraphs'].append({
                'paragraph': paragraph,
                'text': text,
                'row_data': row
            })
        
        # Sort paragraphs within each group by paragraph number
        for group in groups.values():
            try:
                group['paragraphs'].sort(key=lambda x: int(x['paragraph']) if x['paragraph'].isdigit() else 0)
            except:
                # If sorting fails, keep original order
                pass
                
        return groups
    
    def get_voice_for_topic(self, topic_id: str) -> str:
        """Determine voice based on TopicID"""
        topic_id_lower = topic_id.lower()
        if 'a' in topic_id_lower:
            return 'Enceladus'
        elif 'b' in topic_id_lower:
            return 'Sulafat'
        else:
            return 'Enceladus'  # Default voice
    
    def generate_filename(self, topic_id: str, part_number: str, max_parts: int) -> str:
        """Generate filename based on TopicID and PartNumber"""
        topic_id_lower = topic_id.lower()
        
        # Determine prefix
        if 'a' in topic_id_lower:
            prefix = 'a_'
        elif 'b' in topic_id_lower:
            prefix = 'b_'
        else:
            prefix = 'a_'  # Default prefix
        
        # Extract 6-digit number from TopicID
        import re
        numbers = re.findall(r'\d+', topic_id)
        if numbers:
            # Take the first number and pad to 6 digits if needed
            number = numbers[0].zfill(6)
        else:
            number = '000000'
        
        # Generate filename
        filename = f"{prefix}{number}_{part_number}of{max_parts}.wav"
        return filename
    
    async def generate_tts_async(self, text: str, output_file: str, voice: str, model: str, api_key: str):
        """Async method to generate TTS using Gemini API"""
        try:
            import google.genai as genai_new
            
            # Initialize client
            client = genai_new.Client(api_key=api_key)
            
            # Configure speech settings
            speech_config = genai_new.types.SpeechConfig(
                voice_config=genai_new.types.VoiceConfig(
                    prebuilt_voice_config=genai_new.types.PrebuiltVoiceConfig(
                        voice_name=voice
                    )
                )
            )
            
            # Generate TTS
            response = await client.aio.models.generate_content(
                model=model,
                contents=text,
                config=genai_new.types.GenerateContentConfig(
                    response_modalities=["AUDIO"],
                    speech_config=speech_config
                )
            )
            
            # Extract audio data
            audio_data = response.candidates[0].content.parts[0].inline_data.data
            
            # Set up WAV file for writing
            with wave.open(output_file, 'wb') as wf:
                wf.setnchannels(1)  # Mono
                wf.setsampwidth(2)  # 16-bit
                wf.setframerate(24000)  # 24kHz
                wf.writeframes(audio_data)
            
            return True
            
        except Exception as e:
            self.logger.error(f"TTS generation failed: {str(e)}")
            return False
    
    def generate_tts(self, text: str, output_file: str, voice: str, model: str, api_key: str) -> bool:
        """Generate TTS using Gemini API"""
        try:
            # Run the async TTS generation
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(self.generate_tts_async(text, output_file, voice, model, api_key))
            finally:
                loop.close()
                
        except Exception as e:
            self.logger.error(f"TTS generation error: {str(e)}")
            return False
    
    def update_done_status(self, rows_to_update):
        """Update Done status for processed rows"""
        for row in rows_to_update:
            row['Done'] = '1'
    
    def save_updated_csv(self, file_path: str):
        """Save the updated CSV with Done status"""
        try:
            with open(file_path, 'w', newline='', encoding='utf-8-sig') as file:
                if self.csv_data:
                    fieldnames = self.csv_data[0].keys()
                    writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter=';')
                    writer.writeheader()
                    writer.writerows(self.csv_data)
            self.logger.info(f"Updated CSV saved to {file_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error saving updated CSV: {str(e)}")
            return False

class ScriptTTSGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Script TTS Processor")
        self.root.geometry("800x700")
        
        self.processor = ScriptTTSProcessor()
        
        # Variables
        self.csv_file_var = tk.StringVar()
        self.apikey_file_var = tk.StringVar()
        self.output_folder_var = tk.StringVar()
        self.model_var = tk.StringVar(value="gemini-2.5-flash-preview-tts")
        self.status_var = tk.StringVar(value="Ready")
        self.progress_var = tk.DoubleVar()
        
        self.setup_ui()
        
    def setup_ui(self):
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        
        # CSV file selection
        ttk.Label(main_frame, text="Input CSV File:").grid(row=0, column=0, sticky=tk.W, pady=5)
        ttk.Entry(main_frame, textvariable=self.csv_file_var, width=50).grid(row=0, column=1, sticky=(tk.W, tk.E), padx=5)
        ttk.Button(main_frame, text="Browse", command=self.browse_csv_file).grid(row=0, column=2, padx=5)
        
        # API key file selection
        ttk.Label(main_frame, text="API Key CSV File:").grid(row=1, column=0, sticky=tk.W, pady=5)
        ttk.Entry(main_frame, textvariable=self.apikey_file_var, width=50).grid(row=1, column=1, sticky=(tk.W, tk.E), padx=5)
        ttk.Button(main_frame, text="Browse", command=self.browse_apikey_file).grid(row=1, column=2, padx=5)
        
        # Output folder selection
        ttk.Label(main_frame, text="Output Folder:").grid(row=2, column=0, sticky=tk.W, pady=5)
        ttk.Entry(main_frame, textvariable=self.output_folder_var, width=50).grid(row=2, column=1, sticky=(tk.W, tk.E), padx=5)
        ttk.Button(main_frame, text="Browse", command=self.browse_output_folder).grid(row=2, column=2, padx=5)
        
        # Model selection
        ttk.Label(main_frame, text="TTS Model:").grid(row=3, column=0, sticky=tk.W, pady=5)
        model_frame = ttk.Frame(main_frame)
        model_frame.grid(row=3, column=1, sticky=(tk.W, tk.E), padx=5)
        
        ttk.Radiobutton(model_frame, text="gemini-2.5-flash-preview-tts", 
                       variable=self.model_var, value="gemini-2.5-flash-preview-tts").pack(anchor=tk.W)
        ttk.Radiobutton(model_frame, text="gemini-2.5-pro-preview-tts", 
                       variable=self.model_var, value="gemini-2.5-pro-preview-tts").pack(anchor=tk.W)
        
        # Process button
        ttk.Button(main_frame, text="Start TTS Processing", command=self.start_processing).grid(row=4, column=0, columnspan=3, pady=20)
        
        # Progress bar
        self.progress = ttk.Progressbar(main_frame, mode='determinate', variable=self.progress_var)
        self.progress.grid(row=5, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        
        # Status label
        self.status_label = ttk.Label(main_frame, textvariable=self.status_var)
        self.status_label.grid(row=6, column=0, columnspan=3, pady=5)
        
        # Text area for log
        self.log_text = tk.Text(main_frame, height=20, width=80)
        self.log_text.grid(row=7, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S), pady=10)
        
        # Scrollbar for text area
        scrollbar = ttk.Scrollbar(main_frame, orient=tk.VERTICAL, command=self.log_text.yview)
        scrollbar.grid(row=7, column=3, sticky=(tk.N, tk.S))
        self.log_text.configure(yscrollcommand=scrollbar.set)
        
        # Configure grid weights for main_frame
        main_frame.rowconfigure(7, weight=1)
        
    def browse_csv_file(self):
        file = filedialog.askopenfilename(
            title="Select CSV File",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if file:
            self.csv_file_var.set(file)
            
    def browse_apikey_file(self):
        file = filedialog.askopenfilename(
            title="Select API Key CSV File",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if file:
            self.apikey_file_var.set(file)
            
    def browse_output_folder(self):
        folder = filedialog.askdirectory(title="Select Output Folder")
        if folder:
            self.output_folder_var.set(folder)
            
    def log_message(self, message):
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)
        self.root.update()
        
    def validate_inputs(self) -> bool:
        """Validate all input fields"""
        if not self.csv_file_var.get():
            messagebox.showerror("Error", "Please select an input CSV file")
            return False
            
        if not self.apikey_file_var.get():
            messagebox.showerror("Error", "Please select an API key CSV file")
            return False
            
        if not self.output_folder_var.get():
            messagebox.showerror("Error", "Please select an output folder")
            return False
            
        if not os.path.exists(self.csv_file_var.get()):
            messagebox.showerror("Error", "Input CSV file does not exist")
            return False
            
        if not os.path.exists(self.apikey_file_var.get()):
            messagebox.showerror("Error", "API key CSV file does not exist")
            return False
            
        if not os.path.exists(self.output_folder_var.get()):
            messagebox.showerror("Error", "Output folder does not exist")
            return False
            
        return True
    
    def load_files(self) -> bool:
        """Load all input files"""
        try:
            # Load API keys
            if not self.processor.load_api_keys(self.apikey_file_var.get()):
                messagebox.showerror("Error", "Failed to load API keys")
                return False
            
            # Load CSV file
            if not self.processor.load_csv_file(self.csv_file_var.get()):
                messagebox.showerror("Error", "Failed to load CSV file")
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
        """Start the processing in a separate thread"""
        if not self.validate_inputs():
            return
        
        if not self.load_files():
            return
        
        # Disable process button
        self.process_button = None  # We'll find it in the processing thread
        self.status_var.set("Processing...")
        self.log_text.delete(1.0, tk.END)
        
        # Start processing in separate thread
        thread = threading.Thread(target=self.process_files)
        thread.daemon = True
        thread.start()
    
    def process_files(self):
        """Process files in background thread"""
        try:
            self.log_message("Starting TTS processing...")
            
            # Group data by TopicID and PartNumber
            groups = self.processor.group_data_by_topic_and_part()
            total_groups = len(groups)
            
            if total_groups == 0:
                self.log_message("No valid data groups found")
                return
            
            self.log_message(f"Found {total_groups} groups to process")
            
            processed_count = 0
            successful_count = 0
            skipped_count = 0
            
            # Process each group
            for group_key, group_data in groups.items():
                processed_count += 1
                progress = (processed_count / total_groups) * 100
                self.update_progress(progress)
                
                topic_id = group_data['topic_id']
                part_number = group_data['part_number']
                paragraphs = group_data['paragraphs']
                
                self.log_message(f"Processing {group_key} ({processed_count}/{total_groups})")
                
                # Combine texts from all paragraphs
                combined_text = " ".join([p['text'] for p in paragraphs if p['text'].strip()])
                
                if not combined_text.strip():
                    self.log_message(f"  Skipped - no text content")
                    continue
                
                # Determine voice
                voice = self.processor.get_voice_for_topic(topic_id)
                self.log_message(f"  Using voice: {voice}")
                
                # Get API key
                api_key = self.processor.get_next_api_key()
                if not api_key:
                    self.log_message(f"  Error - no API key available")
                    continue
                
                # Generate filename
                # Find max parts for this topic
                max_parts = 1
                for other_key, other_data in groups.items():
                    if other_data['topic_id'] == topic_id:
                        try:
                            max_parts = max(max_parts, int(other_data['part_number']))
                        except:
                            pass
                
                filename = self.processor.generate_filename(topic_id, part_number, max_parts)
                output_path = os.path.join(self.output_folder_var.get(), filename)
                
                self.log_message(f"  Output file: {filename}")
                self.log_message(f"  Text length: {len(combined_text)} characters")
                
                # Check if file already exists
                if os.path.exists(output_path):
                    self.log_message(f"  ✓ File already exists, skipping TTS generation")
                    success = True
                    skipped_count += 1
                else:
                    # Generate TTS
                    success = self.processor.generate_tts(
                        combined_text, 
                        output_path, 
                        voice, 
                        self.model_var.get(), 
                        api_key
                    )
                
                if success:
                    if os.path.exists(output_path):
                        self.log_message(f"  ✓ File already exists, marked as successful")
                    else:
                        self.log_message(f"  ✓ Successfully generated TTS")
                    successful_count += 1
                    
                    # Update Done status for processed rows
                    rows_to_update = [p['row_data'] for p in paragraphs]
                    self.processor.update_done_status(rows_to_update)
                else:
                    self.log_message(f"  ✗ Failed to generate TTS")
            
            # Save updated CSV
            self.log_message("Saving updated CSV file...")
            if self.processor.save_updated_csv(self.csv_file_var.get()):
                self.log_message("✓ Updated CSV saved successfully")
            else:
                self.log_message("✗ Failed to save updated CSV")
            
            # Final status
            generated_count = successful_count - skipped_count
            failed_count = total_groups - successful_count
            
            self.log_message(f"\nProcessing completed!")
            self.log_message(f"Total groups: {total_groups}")
            self.log_message(f"Files already existed (skipped): {skipped_count}")
            self.log_message(f"New files generated: {generated_count}")
            self.log_message(f"Failed: {failed_count}")
            
            self.status_var.set(f"Completed: {successful_count}/{total_groups} successful")
            self.update_progress(100)
            
            messagebox.showinfo("Processing Complete", 
                              f"TTS processing completed!\n\n"
                              f"Total groups: {total_groups}\n"
                              f"Files already existed (skipped): {skipped_count}\n"
                              f"New files generated: {generated_count}\n"
                              f"Failed: {failed_count}")
            
        except Exception as e:
            error_msg = f"Processing error: {str(e)}"
            self.log_message(error_msg)
            self.status_var.set("Error occurred")
            messagebox.showerror("Error", error_msg)
        
        finally:
            self.status_var.set("Ready")

def main():
    root = tk.Tk()
    app = ScriptTTSGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()
