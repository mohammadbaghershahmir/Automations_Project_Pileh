"""
Main GUI for Content Automation Project
Part 1: PDF Upload, Prompt Selection, Model Selection, and AI Studio Integration
"""

import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import customtkinter as ctk
import os
import logging
import threading
from typing import Optional

from api_layer import APIConfig, APIKeyManager, GeminiAPIClient
from pdf_processor import PDFProcessor
from prompt_manager import PromptManager


class ContentAutomationGUI:
    """Main GUI application for content automation"""
    
    def __init__(self):
        # Configure appearance
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        
        # Initialize main window
        self.root = ctk.CTk()
        self.root.title("Content Automation - Part 1")
        self.root.geometry("1000x800")
        self.root.minsize(900, 700)
        
        # Setup logging
        self.setup_logging()
        
        # Initialize components
        self.pdf_processor = PDFProcessor()
        self.prompt_manager = PromptManager()
        self.api_key_manager = APIKeyManager()
        self.api_client = GeminiAPIClient(self.api_key_manager)
        
        # Variables
        self.pdf_path = None
        self.selected_prompt_name = None
        self.custom_prompt = ""
        self.use_custom_prompt = False
        
        # Setup UI
        self.setup_ui()
        
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('content_automation.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def setup_ui(self):
        """Setup the main user interface"""
        # Create main container with scrollable frame
        main_frame = ctk.CTkScrollableFrame(self.root)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Title
        title = ctk.CTkLabel(main_frame, text="Content Automation - Part 1", 
                            font=ctk.CTkFont(size=24, weight="bold"))
        title.pack(pady=(0, 20))
        
        # API Configuration Section
        self.setup_api_section(main_frame)
        
        # PDF Upload Section
        self.setup_pdf_section(main_frame)
        
        # Prompt Selection Section
        self.setup_prompt_section(main_frame)
        
        # Model Selection Section
        self.setup_model_section(main_frame)
        
        # Output Section
        self.setup_output_section(main_frame)
        
        # Control Buttons
        self.setup_controls(main_frame)
        
        # Status Section
        self.setup_status_section(main_frame)
    
    def setup_api_section(self, parent):
        """Setup API configuration section"""
        api_frame = ctk.CTkFrame(parent)
        api_frame.pack(fill="x", pady=(0, 20))
        
        api_label = ctk.CTkLabel(api_frame, text="🔑 API Configuration", 
                                font=ctk.CTkFont(size=18, weight="bold"))
        api_label.pack(pady=(15, 10))
        
        # API Key CSV file selection
        key_frame = ctk.CTkFrame(api_frame)
        key_frame.pack(fill="x", padx=15, pady=5)
        
        ctk.CTkLabel(key_frame, text="API Key CSV File:", 
                    font=ctk.CTkFont(size=12, weight="bold")).pack(anchor="w", padx=10, pady=(10, 5))
        
        self.api_key_file_var = ctk.StringVar()
        api_key_entry = ctk.CTkEntry(key_frame, textvariable=self.api_key_file_var, width=400)
        api_key_entry.pack(side="left", fill="x", expand=True, padx=(10, 5), pady=(0, 5))
        
        ctk.CTkButton(key_frame, text="Browse", command=self.browse_api_key_file, 
                     width=80).pack(side="right", padx=(5, 10), pady=(0, 5))
        
        ctk.CTkLabel(key_frame, text="CSV format: account;project;api_key (API keys will be used in rotation)", 
                    font=ctk.CTkFont(size=10), text_color="gray").pack(anchor="w", padx=10, pady=(0, 10))
        
        # API keys status
        self.api_keys_status_label = ctk.CTkLabel(key_frame, text="No API keys loaded", 
                                                  font=ctk.CTkFont(size=10), text_color="gray")
        self.api_keys_status_label.pack(anchor="w", padx=10, pady=(0, 10))
    
    def setup_pdf_section(self, parent):
        """Setup PDF upload section"""
        pdf_frame = ctk.CTkFrame(parent)
        pdf_frame.pack(fill="x", pady=(0, 20))
        
        pdf_label = ctk.CTkLabel(pdf_frame, text="📄 PDF Upload", 
                                font=ctk.CTkFont(size=18, weight="bold"))
        pdf_label.pack(pady=(15, 10))
        
        # PDF file selection
        file_frame = ctk.CTkFrame(pdf_frame)
        file_frame.pack(fill="x", padx=15, pady=5)
        
        ctk.CTkLabel(file_frame, text="PDF File:", 
                    font=ctk.CTkFont(size=12, weight="bold")).pack(anchor="w", padx=10, pady=(10, 5))
        
        self.pdf_file_var = ctk.StringVar()
        pdf_entry = ctk.CTkEntry(file_frame, textvariable=self.pdf_file_var, width=400)
        pdf_entry.pack(side="left", fill="x", expand=True, padx=(10, 5), pady=(0, 5))
        
        ctk.CTkButton(file_frame, text="Browse", command=self.browse_pdf_file, 
                     width=80).pack(side="right", padx=(5, 10), pady=(0, 5))
        
        # PDF info display
        self.pdf_info_label = ctk.CTkLabel(file_frame, text="No PDF selected", 
                                           font=ctk.CTkFont(size=10), text_color="gray")
        self.pdf_info_label.pack(anchor="w", padx=10, pady=(0, 10))
    
    def setup_prompt_section(self, parent):
        """Setup prompt selection section"""
        prompt_frame = ctk.CTkFrame(parent)
        prompt_frame.pack(fill="x", pady=(0, 20))
        
        prompt_label = ctk.CTkLabel(prompt_frame, text="💬 Prompt Selection", 
                                   font=ctk.CTkFont(size=18, weight="bold"))
        prompt_label.pack(pady=(15, 10))
        
        # Prompt type selection
        type_frame = ctk.CTkFrame(prompt_frame)
        type_frame.pack(fill="x", padx=15, pady=5)
        
        self.prompt_type_var = ctk.StringVar(value="predefined")
        ctk.CTkRadioButton(type_frame, text="Use Predefined Prompt", 
                          variable=self.prompt_type_var, value="predefined",
                          command=self.on_prompt_type_change).pack(anchor="w", padx=10, pady=5)
        ctk.CTkRadioButton(type_frame, text="Use Custom Prompt", 
                          variable=self.prompt_type_var, value="custom",
                          command=self.on_prompt_type_change).pack(anchor="w", padx=10, pady=5)
        
        # Predefined prompt selection
        predefined_frame = ctk.CTkFrame(prompt_frame)
        predefined_frame.pack(fill="x", padx=15, pady=5)
        
        ctk.CTkLabel(predefined_frame, text="Select Predefined Prompt:", 
                    font=ctk.CTkFont(size=12, weight="bold")).pack(anchor="w", padx=10, pady=(10, 5))
        
        prompt_names = self.prompt_manager.get_prompt_names()
        default_value = prompt_names[0] if prompt_names else ""
        self.prompt_combo_var = ctk.StringVar(value=default_value)
        self.prompt_combo = ctk.CTkComboBox(predefined_frame, values=prompt_names, 
                                           variable=self.prompt_combo_var, width=400,
                                           command=self.on_prompt_selected)
        self.prompt_combo.pack(anchor="w", padx=10, pady=(0, 5))
        
        # Prompt preview
        self.prompt_preview = ctk.CTkTextbox(predefined_frame, height=100)
        self.prompt_preview.pack(fill="x", padx=10, pady=(0, 5))
        if prompt_names and default_value:
            preview_text = self.prompt_manager.get_prompt(default_value) or ""
            self.prompt_preview.insert("1.0", preview_text)
            self.selected_prompt_name = default_value
        self.prompt_preview.configure(state="disabled")
        
        # Custom prompt input
        self.custom_frame = ctk.CTkFrame(prompt_frame)
        
        ctk.CTkLabel(self.custom_frame, text="Enter Custom Prompt:", 
                    font=ctk.CTkFont(size=12, weight="bold")).pack(anchor="w", padx=10, pady=(10, 5))
        
        self.custom_prompt_text = ctk.CTkTextbox(self.custom_frame, height=120)
        self.custom_prompt_text.pack(fill="x", padx=10, pady=(0, 10))
        
        # Update visibility based on initial selection
        self.on_prompt_type_change()
    
    def setup_model_section(self, parent):
        """Setup model selection section"""
        model_frame = ctk.CTkFrame(parent)
        model_frame.pack(fill="x", pady=(0, 20))
        
        model_label = ctk.CTkLabel(model_frame, text="🤖 Model Selection", 
                                  font=ctk.CTkFont(size=18, weight="bold"))
        model_label.pack(pady=(15, 10))
        
        model_select_frame = ctk.CTkFrame(model_frame)
        model_select_frame.pack(fill="x", padx=15, pady=(0, 15))
        
        ctk.CTkLabel(model_select_frame, text="Select Gemini Model:", 
                    font=ctk.CTkFont(size=12, weight="bold")).pack(anchor="w", padx=10, pady=(10, 5))
        
        self.model_var = ctk.StringVar(value=APIConfig.TEXT_MODELS[0])
        self.model_combo = ctk.CTkComboBox(model_select_frame, values=APIConfig.TEXT_MODELS, 
                                          variable=self.model_var, width=400)
        self.model_combo.pack(anchor="w", padx=10, pady=(0, 10))
        
        ctk.CTkLabel(model_select_frame, 
                    text="Available models: gemini-2.5-flash, gemini-2.5-pro, gemini-2.0-flash, etc.", 
                    font=ctk.CTkFont(size=10), text_color="gray").pack(anchor="w", padx=10, pady=(0, 10))
    
    def setup_output_section(self, parent):
        """Setup output configuration section"""
        output_frame = ctk.CTkFrame(parent)
        output_frame.pack(fill="x", pady=(0, 20))
        
        output_label = ctk.CTkLabel(output_frame, text="💾 Output Configuration", 
                                   font=ctk.CTkFont(size=18, weight="bold"))
        output_label.pack(pady=(15, 10))
        
        # Output folder selection
        folder_frame = ctk.CTkFrame(output_frame)
        folder_frame.pack(fill="x", padx=15, pady=5)
        
        ctk.CTkLabel(folder_frame, text="Output Folder:", 
                    font=ctk.CTkFont(size=12, weight="bold")).pack(anchor="w", padx=10, pady=(10, 5))
        
        self.output_folder_var = ctk.StringVar()
        output_entry = ctk.CTkEntry(folder_frame, textvariable=self.output_folder_var, width=400)
        output_entry.pack(side="left", fill="x", expand=True, padx=(10, 5), pady=(0, 5))
        
        ctk.CTkButton(folder_frame, text="Browse", command=self.browse_output_folder, 
                     width=80).pack(side="right", padx=(5, 10), pady=(0, 5))
        
        ctk.CTkLabel(folder_frame, text="Responses will be saved as text files in this folder", 
                    font=ctk.CTkFont(size=10), text_color="gray").pack(anchor="w", padx=10, pady=(0, 10))
    
    def setup_controls(self, parent):
        """Setup control buttons"""
        controls_frame = ctk.CTkFrame(parent)
        controls_frame.pack(fill="x", pady=(0, 20))
        
        buttons_frame = ctk.CTkFrame(controls_frame)
        buttons_frame.pack(pady=20)
        
        # Process button
        self.process_btn = ctk.CTkButton(buttons_frame, text="🚀 Process PDF with AI", 
                                        command=self.process_pdf, width=200, height=40,
                                        font=ctk.CTkFont(size=14, weight="bold"))
        self.process_btn.pack(side="left", padx=10)
    
    def setup_status_section(self, parent):
        """Setup status section"""
        status_frame = ctk.CTkFrame(parent)
        status_frame.pack(fill="x", pady=(0, 20))
        
        ctk.CTkLabel(status_frame, text="📊 Status", 
                    font=ctk.CTkFont(size=18, weight="bold")).pack(pady=(15, 10))
        
        self.status_text = ctk.CTkTextbox(status_frame, height=150)
        self.status_text.pack(fill="x", padx=15, pady=(0, 15))
        
        self.update_status("Ready. Please configure API keys and select a PDF file.")
    
    def browse_api_key_file(self):
        """Browse for API key CSV file"""
        filename = filedialog.askopenfilename(
            title="Select API Key CSV File",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if filename:
            self.api_key_file_var.set(filename)
            if self.api_key_manager.load_from_csv(filename):
                num_keys = len(self.api_key_manager.api_keys)
                self.api_keys_status_label.configure(
                    text=f"✓ Loaded {num_keys} API key(s) - Will be used in rotation",
                    text_color="green"
                )
                self.update_status(f"Loaded {num_keys} API key(s) from: {os.path.basename(filename)}")
            else:
                self.api_keys_status_label.configure(
                    text="✗ Failed to load API keys",
                    text_color="red"
                )
                messagebox.showerror("Error", "Failed to load API keys from file")
    
    
    def browse_pdf_file(self):
        """Browse for PDF file"""
        filename = filedialog.askopenfilename(
            title="Select PDF File",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")]
        )
        if filename:
            self.pdf_file_var.set(filename)
            self.validate_pdf(filename)
    
    def browse_output_folder(self):
        """Browse for output folder"""
        folder = filedialog.askdirectory(title="Select Output Folder")
        if folder:
            self.output_folder_var.set(folder)
            self.update_status(f"Output folder set: {folder}")
    
    def validate_pdf(self, file_path: str):
        """Validate PDF file and update UI"""
        is_valid, error_msg, page_count = self.pdf_processor.validate_pdf(file_path)
        
        if is_valid:
            info = self.pdf_processor.get_pdf_info(file_path)
            file_size_mb = info['file_size'] / (1024 * 1024)
            self.pdf_info_label.configure(
                text=f"✓ Valid PDF - {page_count} pages, {file_size_mb:.2f} MB",
                text_color="green"
            )
            self.pdf_path = file_path
            self.update_status(f"PDF validated: {os.path.basename(file_path)} ({page_count} pages)")
        else:
            self.pdf_info_label.configure(
                text=f"✗ {error_msg}",
                text_color="red"
            )
            self.pdf_path = None
            messagebox.showerror("PDF Validation Error", error_msg)
    
    def on_prompt_type_change(self):
        """Handle prompt type change"""
        if self.prompt_type_var.get() == "predefined":
            # Show predefined, hide custom
            predefined_frame = self.prompt_combo.master
            self.prompt_combo.pack(anchor="w", padx=10, pady=(0, 5))
            self.prompt_preview.pack(fill="x", padx=10, pady=(0, 5))
            self.custom_frame.pack_forget()
            self.use_custom_prompt = False
        else:
            # Hide predefined, show custom
            predefined_frame = self.prompt_combo.master
            self.prompt_combo.pack_forget()
            self.prompt_preview.pack_forget()
            self.custom_frame.pack(fill="x", padx=15, pady=5)
            self.use_custom_prompt = True
    
    def on_prompt_selected(self, selected_name: str):
        """Handle predefined prompt selection"""
        prompt_text = self.prompt_manager.get_prompt(selected_name)
        if prompt_text:
            self.prompt_preview.configure(state="normal")
            self.prompt_preview.delete("1.0", tk.END)
            self.prompt_preview.insert("1.0", prompt_text)
            self.prompt_preview.configure(state="disabled")
            self.selected_prompt_name = selected_name
    
    def get_selected_prompt(self) -> Optional[str]:
        """Get the selected prompt text"""
        if self.use_custom_prompt:
            prompt = self.custom_prompt_text.get("1.0", tk.END).strip()
            return prompt if prompt else None
        else:
            return self.prompt_manager.get_prompt(self.prompt_combo_var.get())
    
    def update_status(self, message: str):
        """Update status text"""
        self.status_text.delete("1.0", tk.END)
        self.status_text.insert("1.0", f"[{self.get_timestamp()}] {message}")
    
    def get_timestamp(self) -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().strftime("%H:%M:%S")
    
    def process_pdf(self):
        """Process PDF with selected prompt and model"""
        def worker():
            try:
                # Disable process button
                self.process_btn.configure(state="disabled", text="Processing...")
                
                # Validate inputs
                if not self.pdf_path or not os.path.exists(self.pdf_path):
                    self.update_status("Error: Please select a valid PDF file")
                    messagebox.showerror("Error", "Please select a valid PDF file")
                    return
                
                prompt = self.get_selected_prompt()
                if not prompt:
                    self.update_status("Error: Please select or enter a prompt")
                    messagebox.showerror("Error", "Please select or enter a prompt")
                    return
                
                # Validate API keys are loaded
                if not self.api_key_manager.api_keys:
                    if not self.api_key_file_var.get():
                        self.update_status("Error: Please load API keys from CSV file")
                        messagebox.showerror("Error", "Please load API keys from CSV file")
                        return
                    else:
                        if not self.api_key_manager.load_from_csv(self.api_key_file_var.get()):
                            self.update_status("Error: Failed to load API keys")
                            messagebox.showerror("Error", "Failed to load API keys from CSV file")
                            return
                
                # Get selected model
                model_name = self.model_var.get()
                
                # Get current API key info (for logging)
                current_key_info = self.api_key_manager.get_current_key_info()
                if current_key_info:
                    self.update_status(f"Using API key: {current_key_info['account']} ({len(self.api_key_manager.api_keys)} keys available)")
                
                self.update_status(f"Processing PDF with {model_name}...")
                self.update_status(f"Prompt length: {len(prompt)} characters")
                self.update_status(f"PDF file: {os.path.basename(self.pdf_path)}")
                self.logger.info(f"=== Starting PDF Processing ===")
                self.logger.info(f"Model: {model_name}")
                self.logger.info(f"PDF: {self.pdf_path}")
                self.logger.info(f"Full prompt being sent ({len(prompt)} chars): {prompt}")
                
                # Process PDF with API (API key will be rotated automatically)
                response = self.api_client.process_pdf_with_prompt(
                    pdf_path=self.pdf_path,
                    prompt=prompt,
                    model_name=model_name
                )
                
                if response:
                    response_length = len(response)
                    
                    # Check if response was truncated
                    is_truncated = getattr(self.api_client, '_response_truncated', False)
                    
                    if is_truncated:
                        self.update_status("❌ WARNING: Response was TRUNCATED!")
                        self.update_status("Response is INCOMPLETE due to MAX_TOKENS limit")
                        messagebox.showwarning("Response Truncated", 
                                             "⚠️ IMPORTANT: The response was TRUNCATED due to MAX_TOKENS limit!\n\n"
                                             "The response is INCOMPLETE.\n\n"
                                             "Solutions:\n"
                                             "1. Use gemini-2.5-pro model (better for long responses)\n"
                                             "2. Simplify your prompt\n"
                                             "3. Break the task into smaller parts\n\n"
                                             "Check the log file for details.")
                    
                    self.update_status("✓ PDF processed successfully!")
                    self.update_status(f"Response length: {response_length} characters")
                    if is_truncated:
                        self.update_status(f"⚠️ TRUNCATED - Response incomplete!")
                    
                    self.logger.info(f"=== Response Received ===")
                    self.logger.info(f"Response length: {response_length} characters")
                    self.logger.info(f"Truncated: {is_truncated}")
                    self.logger.info(f"Response preview (first 500 chars): {response[:500]}...")
                    self.logger.info(f"Response preview (last 500 chars): ...{response[-500:]}")
                    
                    # Check if response seems incomplete
                    if response_length < 200:
                        self.update_status("⚠ Warning: Response seems very short!")
                        self.logger.warning(f"Response is very short ({response_length} chars) - might be incomplete")
                    
                    self.update_status(f"\n📄 Full response ({response_length:,} characters) is being displayed in a new window...")
                    self.update_status(f"Response preview (first 1000 chars):\n{response[:1000]}...")
                    
                    # First, show FULL response in a new window (user can see complete response)
                    self.show_response_window(response, None, is_truncated)
                    
                    # Then automatically save it
                    self.update_status("💾 Saving response to file...")
                    saved_path = self.save_response(response)
                    if saved_path:
                        self.update_status(f"✓ Response saved successfully to: {saved_path}")
                        if is_truncated:
                            self.update_status("⚠️ Note: Saved response is INCOMPLETE (truncated)")
                        self.logger.info(f"Response displayed and saved to: {saved_path}")
                    else:
                        self.update_status("⚠️ Warning: Could not save response automatically")
                        self.logger.warning("Failed to save response automatically")
                else:
                    self.update_status("✗ Failed to process PDF. Check logs for details.")
                    messagebox.showerror("Error", "Failed to process PDF. Check status and logs.")
                
            except Exception as e:
                error_msg = f"Error processing PDF: {str(e)}"
                self.update_status(error_msg)
                self.logger.error(error_msg, exc_info=True)
                messagebox.showerror("Error", error_msg)
            finally:
                # Re-enable process button
                self.process_btn.configure(state="normal", text="🚀 Process PDF with AI")
        
        # Run in separate thread to prevent UI blocking
        threading.Thread(target=worker, daemon=True).start()
    
    def save_response(self, response: str) -> Optional[str]:
        """
        Save response to Word document (.docx)
        
        Args:
            response: Response text to save
            
        Returns:
            Path to saved file or None if failed
        """
        try:
            # Get output folder
            output_folder = self.output_folder_var.get().strip()
            if not output_folder:
                # Use default: same folder as PDF or current directory
                if self.pdf_path:
                    output_folder = os.path.dirname(self.pdf_path)
                else:
                    output_folder = os.getcwd()
            
            # Create output folder if it doesn't exist
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)
            
            # Generate filename based on PDF name and timestamp
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if self.pdf_path:
                pdf_name = os.path.splitext(os.path.basename(self.pdf_path))[0]
                filename = f"{pdf_name}_response_{timestamp}.docx"
            else:
                filename = f"response_{timestamp}.docx"
            
            file_path = os.path.join(output_folder, filename)
            
            # Save as Word document
            try:
                from docx import Document
                from docx.shared import Pt, RGBColor
                from docx.enum.text import WD_ALIGN_PARAGRAPH
                
                doc = Document()
                
                # Title
                title = doc.add_heading('PDF Processing Response', 0)
                title.alignment = WD_ALIGN_PARAGRAPH.CENTER
                
                # Metadata section
                doc.add_paragraph().add_run('Metadata').bold = True
                metadata_para = doc.add_paragraph()
                metadata_para.add_run(f'Generated: ').bold = True
                metadata_para.add_run(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
                if self.pdf_path:
                    metadata_para = doc.add_paragraph()
                    metadata_para.add_run(f'PDF File: ').bold = True
                    metadata_para.add_run(os.path.basename(self.pdf_path))
                
                metadata_para = doc.add_paragraph()
                metadata_para.add_run(f'Model: ').bold = True
                metadata_para.add_run(self.model_var.get())
                
                # Full prompt
                prompt = self.get_selected_prompt()
                if prompt:
                    metadata_para = doc.add_paragraph()
                    metadata_para.add_run(f'Prompt: ').bold = True
                    doc.add_paragraph(prompt)
                
                # Separator
                doc.add_paragraph('=' * 80)
                
                # Response content
                doc.add_paragraph().add_run('Response').bold = True
                
                # Split response into paragraphs and add them
                response_paragraphs = response.split('\n\n')
                for para_text in response_paragraphs:
                    if para_text.strip():
                        # Preserve formatting for lists and special content
                        if para_text.strip().startswith('-') or para_text.strip().startswith('*'):
                            # List item
                            doc.add_paragraph(para_text.strip(), style='List Bullet')
                        elif para_text.strip().startswith(tuple('123456789')):
                            # Numbered list
                            doc.add_paragraph(para_text.strip(), style='List Number')
                        else:
                            # Regular paragraph
                            doc.add_paragraph(para_text.strip())
                
                # Save document
                doc.save(file_path)
                self.logger.info(f"Response saved to Word document: {file_path}")
                return file_path
                
            except ImportError:
                # Fallback to text file if python-docx is not available
                self.logger.warning("python-docx not available, saving as text file")
                txt_file_path = file_path.replace('.docx', '.txt')
                
                # Get prompt again for fallback
                prompt_text = self.get_selected_prompt() or "N/A"
                
                with open(txt_file_path, 'w', encoding='utf-8') as f:
                    f.write(f"PDF Processing Response\n")
                    f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    if self.pdf_path:
                        f.write(f"PDF File: {os.path.basename(self.pdf_path)}\n")
                    f.write(f"Model: {self.model_var.get()}\n")
                    f.write(f"Prompt: {prompt_text}\n")
                    f.write(f"{'='*80}\n\n")
                    f.write(response)
                return txt_file_path
            
        except Exception as e:
            self.logger.error(f"Error saving response: {str(e)}")
            return None
    
    def show_response_window(self, response: str, saved_path: Optional[str] = None, is_truncated: bool = False):
        """Show full response in a new window"""
        response_window = ctk.CTkToplevel(self.root)
        response_window.title("AI Response - Full Content" + (" [TRUNCATED]" if is_truncated else ""))
        # Make window larger for better viewing
        response_window.geometry("1000x700")
        
        # Title
        title_text = "AI Response - Full Content" + (" ⚠️ [TRUNCATED - INCOMPLETE]" if is_truncated else "")
        title_color = "red" if is_truncated else "white"
        title = ctk.CTkLabel(response_window, text=title_text, 
                            font=ctk.CTkFont(size=20, weight="bold"),
                            text_color=title_color)
        title.pack(pady=10)
        
        # Response length info
        response_length = len(response)
        length_label = ctk.CTkLabel(response_window, 
                                   text=f"Response length: {response_length:,} characters", 
                                   font=ctk.CTkFont(size=11), 
                                   text_color="gray")
        length_label.pack(pady=2)
        
        # Warning if truncated
        if is_truncated:
            warning_label = ctk.CTkLabel(response_window, 
                                       text="⚠️ WARNING: Response was TRUNCATED due to MAX_TOKENS limit!\n"
                                            "The response is INCOMPLETE. Consider using gemini-2.5-pro model.", 
                                       font=ctk.CTkFont(size=12, weight="bold"),
                                       text_color="red",
                                       wraplength=900)
            warning_label.pack(pady=5)
        
        # Saved path info (will be updated if saved later)
        saved_label = None
        if saved_path:
            saved_label = ctk.CTkLabel(response_window, 
                                       text=f"✓ Saved to: {os.path.basename(saved_path)}", 
                                       font=ctk.CTkFont(size=10), text_color="green")
            saved_label.pack(pady=5)
        
        # Response text - Full content with scrollbar
        # Use a frame to contain the textbox and scrollbar
        text_frame = ctk.CTkFrame(response_window)
        text_frame.pack(fill="both", expand=True, padx=20, pady=10)
        
        response_text = ctk.CTkTextbox(text_frame, 
                                      height=500,
                                      wrap="word",  # Wrap text at word boundaries
                                      font=ctk.CTkFont(size=11))
        response_text.pack(fill="both", expand=True)
        
        # Insert full response
        response_text.insert("1.0", response)
        response_text.configure(state="normal")  # Allow copying and scrolling
        
        # Scroll to top
        response_text.see("1.0")
        
        # Buttons frame
        buttons_frame = ctk.CTkFrame(response_window)
        buttons_frame.pack(pady=10)
        
        # Save button (if not already saved)
        if saved_path:
            ctk.CTkButton(buttons_frame, text="📁 Open Folder", 
                         command=lambda: self.open_folder(os.path.dirname(saved_path)),
                         width=140).pack(side="left", padx=5)
        else:
            ctk.CTkButton(buttons_frame, text="💾 Save As...", 
                         command=lambda: self.save_response_as(response, response_window),
                         width=140).pack(side="left", padx=5)
        
        # Copy button
        ctk.CTkButton(buttons_frame, text="📋 Copy All", 
                     command=lambda: self.copy_to_clipboard(response, response_window),
                     width=120).pack(side="left", padx=5)
        
        # Close button
        ctk.CTkButton(buttons_frame, text="✕ Close", command=response_window.destroy,
                     width=100).pack(side="left", padx=5)
        
        # Store reference to update saved path later if needed
        response_window._saved_label = saved_label
        response_window._saved_path = saved_path
    
    def copy_to_clipboard(self, text: str, parent_window):
        """Copy text to clipboard"""
        try:
            parent_window.clipboard_clear()
            parent_window.clipboard_append(text)
            messagebox.showinfo("Copied", "Response copied to clipboard!")
        except Exception as e:
            self.logger.error(f"Error copying to clipboard: {str(e)}")
            messagebox.showerror("Error", f"Could not copy to clipboard: {str(e)}")
    
    def save_response_as(self, response: str, parent_window):
        """Save response to a custom location"""
        filename = filedialog.asksaveasfilename(
            title="Save Response As",
            defaultextension=".docx",
            filetypes=[("Word documents", "*.docx"), ("Text files", "*.txt"), ("All files", "*.*")]
        )
        if filename:
            try:
                if filename.lower().endswith('.docx'):
                    # Save as Word document
                    from docx import Document
                    from datetime import datetime
                    
                    doc = Document()
                    doc.add_heading('PDF Processing Response', 0)
                    
                    # Metadata
                    doc.add_paragraph().add_run('Metadata').bold = True
                    para = doc.add_paragraph()
                    para.add_run(f'Generated: ').bold = True
                    para.add_run(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    if self.pdf_path:
                        para = doc.add_paragraph()
                        para.add_run(f'PDF File: ').bold = True
                        para.add_run(os.path.basename(self.pdf_path))
                    
                    para = doc.add_paragraph()
                    para.add_run(f'Model: ').bold = True
                    para.add_run(self.model_var.get())
                    
                    # Response
                    doc.add_paragraph().add_run('Response').bold = True
                    response_paragraphs = response.split('\n\n')
                    for para_text in response_paragraphs:
                        if para_text.strip():
                            doc.add_paragraph(para_text.strip())
                    
                    doc.save(filename)
                else:
                    # Save as text file
                    with open(filename, 'w', encoding='utf-8') as f:
                        f.write(response)
                
                messagebox.showinfo("Success", f"Response saved to:\n{filename}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save file: {str(e)}")
    
    def open_folder(self, folder_path: str):
        """Open folder in file manager"""
        try:
            import subprocess
            import platform
            
            if platform.system() == "Windows":
                os.startfile(folder_path)
            elif platform.system() == "Darwin":  # macOS
                subprocess.Popen(["open", folder_path])
            else:  # Linux
                subprocess.Popen(["xdg-open", folder_path])
        except Exception as e:
            self.logger.error(f"Error opening folder: {str(e)}")
            messagebox.showerror("Error", f"Could not open folder: {str(e)}")
    
    def run(self):
        """Run the application"""
        self.root.mainloop()


def main():
    """Main entry point"""
    try:
        app = ContentAutomationGUI()
        app.run()
    except Exception as e:
        print(f"Application error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

