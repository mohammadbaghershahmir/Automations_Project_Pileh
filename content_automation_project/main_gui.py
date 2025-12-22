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
import csv
import io
import json
import glob
from datetime import datetime
from typing import Optional, List, Dict, Any

from api_layer import APIConfig, APIKeyManager, GeminiAPIClient
from pdf_processor import PDFProcessor
from prompt_manager import PromptManager
from multi_part_processor import MultiPartProcessor
from multi_part_post_processor import MultiPartPostProcessor
from third_stage_converter import ThirdStageConverter
from txt_stage_json_utils import load_stage_txt_as_json
from stage_e_processor import StageEProcessor
from stage_f_processor import StageFProcessor
from stage_j_processor import StageJProcessor
from stage_h_processor import StageHProcessor
from stage_m_processor import StageMProcessor
from stage_l_processor import StageLProcessor
from stage_v_processor import StageVProcessor


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

        # Common Farsi-friendly font for textboxes
        try:
            self.farsi_text_font = ctk.CTkFont(family="Tahoma", size=11)
        except Exception:
            # Fallback if Tahoma is not available
            self.farsi_text_font = ctk.CTkFont(size=11)
        
        # Setup logging
        self.setup_logging()
        
        # Initialize components
        self.pdf_processor = PDFProcessor()
        self.prompt_manager = PromptManager()
        self.api_key_manager = APIKeyManager()
        self.api_client = GeminiAPIClient(self.api_key_manager)
        self.multi_part_processor = MultiPartProcessor(self.api_client)
        self.multi_part_post_processor = MultiPartPostProcessor(self.api_client)
        self.stage_e_processor = StageEProcessor(self.api_client)
        self.stage_f_processor = StageFProcessor(self.api_client)
        self.stage_j_processor = StageJProcessor(self.api_client)
        self.stage_h_processor = StageHProcessor(self.api_client)
        self.stage_m_processor = StageMProcessor(self.api_client)
        self.stage_l_processor = StageLProcessor(self.api_client)
        self.stage_v_processor = StageVProcessor(self.api_client)
        
        # Variables
        self.pdf_path = None
        self.selected_prompt_name = None
        self.custom_prompt = ""
        self.use_custom_prompt = False
        self.last_final_output_path = None       # Stage 1 JSON
        self.last_post_processed_path = None     # Stage 2 JSON
        self.last_corrected_path = None          # Stage 3 JSON (with PointId)
        self.last_stage3_raw_path = None         # Stage 3 raw JSON (new intermediate stage)
        self.last_stage4_raw_path = None         # Stage 4 raw JSON (chunked model output)
        self.last_stage_e_path = None            # Stage E JSON
        self.last_stage_j_path = None            # Stage J JSON
        self.last_stage_f_path = None            # Stage F JSON
        
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
        # Create TabView for all stages (including Stages 1-4)
        self.main_tabview = ctk.CTkTabview(self.root)
        # Initially hide tabview - show main frame first
        self.main_tabview.pack_forget()
        
        # Tab 1: Stages 1-4 (for tabview access) - MUST be first to be default
        self.tab_stages_1_4 = self.main_tabview.add("Stages 1-4")
        
        # Main page: Stages 1-4 (Original UI) - shown directly on root
        # This is the Part 1 form (Stage 1 only)
        self.main_stages_1_4_frame = ctk.CTkFrame(self.root)
        self.main_stages_1_4_frame.pack(fill="both", expand=True, padx=10, pady=10)
        self.setup_stages_1_4_ui(self.main_stages_1_4_frame)
        
        # Now setup Stages 2-3-4 UI in tabview (the actual Stages 1-4 form)
        self.setup_stages_2_3_4_ui(self.tab_stages_1_4)
        
        # Tab 2: Stage E
        self.tab_stage_e = self.main_tabview.add("Stage E")
        self.setup_stage_e_ui(self.tab_stage_e)
        
        # Tab 3: Stage F
        self.tab_stage_f = self.main_tabview.add("Stage F")
        self.setup_stage_f_ui(self.tab_stage_f)
        
        # Tab 4: Stage J
        self.tab_stage_j = self.main_tabview.add("Stage J")
        self.setup_stage_j_ui(self.tab_stage_j)
        
        # Tab 5: Stage H
        self.tab_stage_h = self.main_tabview.add("Stage H")
        self.setup_stage_h_ui(self.tab_stage_h)
        
        # Tab 6: Stage V
        self.tab_stage_v = self.main_tabview.add("Stage V")
        self.setup_stage_v_ui(self.tab_stage_v)
        
        # Tab 7: Stage M
        self.tab_stage_m = self.main_tabview.add("Stage M")
        self.setup_stage_m_ui(self.tab_stage_m)
        
        # Tab 8: Stage L
        self.tab_stage_l = self.main_tabview.add("Stage L")
        self.setup_stage_l_ui(self.tab_stage_l)
        
        # Ensure Stages 1-4 is the default/selected tab
        self.main_tabview.set("Stages 1-4")
        
        # Pipeline Status Bar (shown on all tabs)
        self.setup_pipeline_status_bar()
    
    def setup_stages_1_4_ui(self, parent):
        """Setup UI for Stages 1-4 (original functionality)"""
        # Check if this is the tabview version (not main view)
        is_tabview = hasattr(self, 'main_stages_1_4_frame') and parent != self.main_stages_1_4_frame
        
        # Create main container with scrollable frame
        main_frame = ctk.CTkScrollableFrame(parent)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Add navigation button only in tabview version
        if is_tabview:
            nav_frame = ctk.CTkFrame(main_frame)
            nav_frame.pack(fill="x", pady=(0, 10))
            ctk.CTkButton(
                nav_frame,
                text="< Back to Main View",
                command=self.show_main_view,
                width=150,
                height=30,
                font=ctk.CTkFont(size=12),
                fg_color="gray",
                hover_color="darkgray"
            ).pack(side="left", padx=10, pady=5)
        
        # Setup all sections - widgets will be shared via StringVar and other variables
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
        
        api_label = ctk.CTkLabel(api_frame, text="API Configuration", 
                                font=ctk.CTkFont(size=18, weight="bold"))
        api_label.pack(pady=(15, 10))
        
        # API Key CSV file selection
        key_frame = ctk.CTkFrame(api_frame)
        key_frame.pack(fill="x", padx=15, pady=5)
        
        ctk.CTkLabel(key_frame, text="API Key CSV File:", 
                    font=ctk.CTkFont(size=12, weight="bold")).pack(anchor="w", padx=10, pady=(10, 5))
        
        # Only create StringVar if it doesn't exist (for main view)
        if not hasattr(self, 'api_key_file_var'):
            self.api_key_file_var = ctk.StringVar()
        api_key_entry = ctk.CTkEntry(key_frame, textvariable=self.api_key_file_var, width=400)
        api_key_entry.pack(side="left", fill="x", expand=True, padx=(10, 5), pady=(0, 5))
        
        ctk.CTkButton(key_frame, text="Browse", command=self.browse_api_key_file, 
                     width=80).pack(side="right", padx=(5, 10), pady=(0, 5))
        
        ctk.CTkLabel(key_frame, text="CSV format: account;project;api_key (API keys will be used in rotation)", 
                    font=ctk.CTkFont(size=10), text_color="gray").pack(anchor="w", padx=10, pady=(0, 10))
        
        # API keys status - only create for main view
        if not hasattr(self, 'api_keys_status_label') or parent == self.main_stages_1_4_frame:
            if not hasattr(self, 'api_keys_status_label'):
                self.api_keys_status_label = ctk.CTkLabel(key_frame, text="No API keys loaded", 
                                                          font=ctk.CTkFont(size=10), text_color="gray")
                self.api_keys_status_label.pack(anchor="w", padx=10, pady=(0, 10))
        else:
            # For tabview, create a separate label that syncs with main
            api_keys_status_label_tab = ctk.CTkLabel(key_frame, text="No API keys loaded", 
                                                      font=ctk.CTkFont(size=10), text_color="gray")
            api_keys_status_label_tab.pack(anchor="w", padx=10, pady=(0, 10))
    
    def setup_pdf_section(self, parent):
        """Setup PDF upload section"""
        pdf_frame = ctk.CTkFrame(parent)
        pdf_frame.pack(fill="x", pady=(0, 20))
        
        pdf_label = ctk.CTkLabel(pdf_frame, text="PDF Upload", 
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
        
        prompt_label = ctk.CTkLabel(prompt_frame, text="Prompt Selection", 
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
        self.prompt_preview = ctk.CTkTextbox(predefined_frame, height=100, font=self.farsi_text_font)
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
        
        self.custom_prompt_text = ctk.CTkTextbox(self.custom_frame, height=120, font=self.farsi_text_font)
        self.custom_prompt_text.pack(fill="x", padx=10, pady=(0, 10))
        
        # Update visibility based on initial selection
        self.on_prompt_type_change()
    
    def setup_model_section(self, parent):
        """Setup model selection section"""
        model_frame = ctk.CTkFrame(parent)
        model_frame.pack(fill="x", pady=(0, 20))
        
        model_label = ctk.CTkLabel(model_frame, text="Model Selection", 
                                  font=ctk.CTkFont(size=18, weight="bold"))
        model_label.pack(pady=(15, 10))
        
        model_select_frame = ctk.CTkFrame(model_frame)
        model_select_frame.pack(fill="x", padx=15, pady=(0, 15))
        
        ctk.CTkLabel(model_select_frame, text="Select Gemini Model:", 
                    font=ctk.CTkFont(size=12, weight="bold")).pack(anchor="w", padx=10, pady=(10, 5))
        
        # Only create StringVar if it doesn't exist (for main view)
        if not hasattr(self, 'model_var'):
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
        
        output_label = ctk.CTkLabel(output_frame, text="Output Configuration", 
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
        self.process_btn = ctk.CTkButton(buttons_frame, text="Process PDF with AI", 
                                        command=self.process_pdf, width=200, height=40,
                                        font=ctk.CTkFont(size=14, weight="bold"))
        self.process_btn.pack(side="left", padx=10)
        
        # View CSV button
        self.view_csv_btn = ctk.CTkButton(buttons_frame, text="View CSV", 
                                         command=self.view_csv_from_json, width=150, height=40,
                                         font=ctk.CTkFont(size=14),
                                         fg_color="green", hover_color="darkgreen")
        self.view_csv_btn.pack(side="left", padx=10)

        # View/Edit Stage 1 JSON button
        self.view_stage1_json_btn = ctk.CTkButton(
            buttons_frame,
            text="View/Edit Stage 1 JSON",
            command=lambda: self.open_json_editor(self.last_final_output_path),
            width=220,
            height=40,
            font=ctk.CTkFont(size=14),
        )
        self.view_stage1_json_btn.pack(side="left", padx=10)
        
        # Navigation button to switch to tabview
        self.show_tabview_btn = ctk.CTkButton(
            buttons_frame,
            text="View Other Stages (E-L)",
            command=self.show_tabview,
            width=200,
            height=40,
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color="purple",
            hover_color="darkviolet"
        )
        self.show_tabview_btn.pack(side="left", padx=10)
    
    def setup_status_section(self, parent):
        """Setup status section"""
        status_frame = ctk.CTkFrame(parent)
        status_frame.pack(fill="x", pady=(0, 20))
        
        ctk.CTkLabel(status_frame, text="Status", 
                    font=ctk.CTkFont(size=18, weight="bold")).pack(pady=(15, 10))
        
        self.status_text = ctk.CTkTextbox(status_frame, height=150, font=self.farsi_text_font)
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
                    text=f"Loaded {num_keys} API key(s) - Will be used in rotation",
                    text_color="green"
                )
                self.update_status(f"Loaded {num_keys} API key(s) from: {os.path.basename(filename)}")
            else:
                self.api_keys_status_label.configure(
                    text="X Failed to load API keys",
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
                text=f"Valid PDF - {page_count} pages, {file_size_mb:.2f} MB",
                text_color="green"
            )
            self.pdf_path = file_path
            self.update_status(f"PDF validated: {os.path.basename(file_path)} ({page_count} pages)")
        else:
            self.pdf_info_label.configure(
                text=f"X {error_msg}",
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
                
                # Process PDF using multi-part processor
                # This handles large outputs by splitting into multiple parts
                self.update_status("Processing PDF with multi-part system...")
                self.update_status("Large outputs will be split into multiple parts")
                self.update_status("Only the final combined JSON will be displayed")
                
                def progress_callback(message: str):
                    """Callback for progress updates during multi-part processing"""
                    self.update_status(message)
                
                # Use multi-part processor
                final_output_path = self.multi_part_processor.process_multi_part(
                    pdf_path=self.pdf_path,
                    base_prompt=prompt,  # User's original prompt (no modifications)
                    model_name=model_name,
                    temperature=0.7,
                    resume=True,  # Enable resume capability
                    progress_callback=progress_callback
                )
                
                if final_output_path and os.path.exists(final_output_path):
                    # Load final output JSON
                    try:
                        with open(final_output_path, 'r', encoding='utf-8') as f:
                            final_data = json.load(f)
                        
                        # Extract metadata and rows
                        metadata = final_data.get('metadata', {})
                        rows = final_data.get('rows', [])
                        
                        total_parts = metadata.get('total_parts', 0)
                        total_rows = metadata.get('total_rows', len(rows))
                        
                        self.update_status("Multi-part processing completed successfully.")
                        self.update_status(f"Total parts processed: {total_parts}")
                        self.update_status(f"Total rows extracted: {total_rows}")
                        self.update_status(f"Final output saved to: {final_output_path}")
                        
                        # Store final output path for CSV viewing
                        self.last_final_output_path = final_output_path
                        
                        # Display final output (not individual parts)
                        if rows:
                            self.update_status(f"\nDisplaying final combined output ({total_rows} rows)...")
                            # Show JSON table
                            response_text = json.dumps(final_data, ensure_ascii=False, indent=2)
                            self.show_response_window(response_text, final_output_path, is_csv=False, is_json=True)
                        else:
                            self.update_status("Warning: Final output contains no rows.")
                            messagebox.showwarning("No Data", "The final output file contains no rows. Check the logs for details.")
                            return
                        
                    except json.JSONDecodeError as e:
                        self.update_status(f"Error: Failed to parse final output JSON: {str(e)}")
                        self.logger.error(f"Failed to parse final output: {str(e)}")
                        messagebox.showerror("Error", f"Failed to parse final output JSON: {str(e)}")
                        return
                    except Exception as e:
                        self.update_status(f"Error loading final output: {str(e)}")
                        self.logger.error(f"Error loading final output: {str(e)}", exc_info=True)
                        messagebox.showerror("Error", f"Failed to load final output: {str(e)}")
                        return
                else:
                    self.update_status("❌ Multi-part processing failed or was incomplete")
                    self.logger.error("Multi-part processing returned no final output file")
                    messagebox.showerror("Error", "Multi-part processing failed. Check the logs for details.")
                    return
                
            except Exception as e:
                error_msg = f"Error processing PDF: {str(e)}"
                self.update_status(error_msg)
                self.logger.error(error_msg, exc_info=True)
                messagebox.showerror("Error", error_msg)
            finally:
                # Re-enable process button
                self.process_btn.configure(state="normal", text="Process PDF with AI")
        
        # Run in separate thread to prevent UI blocking
        threading.Thread(target=worker, daemon=True).start()
    
    def view_csv_from_json(self):
        """Convert the last processed JSON file to CSV format and display it"""
        if not self.last_final_output_path or not os.path.exists(self.last_final_output_path):
            messagebox.showerror("Error", "No JSON file available. Please process a PDF first.")
            return
        
        try:
            # Load JSON file
            with open(self.last_final_output_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            # Extract rows from JSON
            rows = json_data.get('rows', [])
            if not rows:
                messagebox.showwarning("No Data", "The JSON file contains no rows.")
                return
            
            # Convert to CSV format with ";;;" delimiter
            delimiter = ";;;"
            
            # Get headers from first row
            if not isinstance(rows, list) or len(rows) == 0:
                messagebox.showerror("Error", "Invalid JSON format: no rows found")
                return
            
            headers = list(rows[0].keys())
            
            # Build CSV with header first
            csv_lines = [delimiter.join(headers)]
            
            # Add data rows
            for row in rows:
                csv_line = delimiter.join(str(row.get(h, "")) for h in headers)
                csv_lines.append(csv_line)
            
            csv_text = "\n".join(csv_lines)
            
            # Display CSV in response window
            csv_file_path = self.last_final_output_path.replace('.json', '.csv')
            self.show_response_window(csv_text, csv_file_path, is_csv=True, is_json=False)
            
        except json.JSONDecodeError as e:
            messagebox.showerror("Error", f"Failed to parse JSON file:\n{str(e)}")
            self.logger.error(f"Failed to parse JSON: {str(e)}")
        except Exception as e:
            messagebox.showerror("Error", f"Error converting JSON to CSV:\n{str(e)}")
            self.logger.error(f"Error converting JSON to CSV: {str(e)}", exc_info=True)
    
    def update_status(self, message: str):
        """Update status text"""
        self.status_text.delete("1.0", tk.END)
        self.status_text.insert("1.0", f"[{self.get_timestamp()}] {message}")
    
    def get_timestamp(self) -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().strftime("%H:%M:%S")

    def open_part_processing_window(self):
        """
        Open second page for part-by-part processing of an existing final_output.json.
        This window now also controls automatic Stage 3 and Stage 4 processing per Part.
        """
        window = ctk.CTkToplevel(self.root)
        window.title("Part Processing - Stages 2 → 3 → 4")
        window.geometry("950x850")
        
        main_frame = ctk.CTkScrollableFrame(window)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        title = ctk.CTkLabel(
            main_frame,
            text="Second Stage & Automatic Pipeline (Stages 2 to 3 to 4)",
            font=ctk.CTkFont(size=22, weight="bold"),
        )
        title.pack(pady=(0, 20))
        
        #
        # --- Stage 2 configuration (per-Part processing, قبلی) ---
        #
        stage2_frame = ctk.CTkFrame(main_frame)
        stage2_frame.pack(fill="x", pady=(0, 20))
        
        ctk.CTkLabel(
            stage2_frame,
            text="Second-Stage Prompt (per Part)",
            font=ctk.CTkFont(size=18, weight="bold"),
        ).pack(pady=(15, 10))
        
        ctk.CTkLabel(
            stage2_frame,
            text="This prompt will be applied separately to each Part in Stage 2.\n"
                 "The JSON rows for each Part will be sent to the model with this prompt.",
            font=ctk.CTkFont(size=11),
            text_color="gray",
        ).pack(anchor="w", padx=10, pady=(0, 10))
        
        self.second_stage_prompt_text = ctk.CTkTextbox(stage2_frame, height=140, font=self.farsi_text_font)
        self.second_stage_prompt_text.pack(fill="x", padx=10, pady=(0, 10))
        
        # Pre-fill with current prompt if available
        current_prompt = self.get_selected_prompt()
        if current_prompt:
            self.second_stage_prompt_text.insert("1.0", current_prompt)
        
        # Chapter name section (right after prompt)
        chapter_frame = ctk.CTkFrame(stage2_frame)
        chapter_frame.pack(fill="x", padx=10, pady=(0, 10))
        
        ctk.CTkLabel(
            chapter_frame,
            text="Chapter Name:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        self.second_stage_chapter_var = ctk.StringVar(value="درمان با UV")
        chapter_entry = ctk.CTkEntry(
            chapter_frame,
            textvariable=self.second_stage_chapter_var,
            width=400
        )
        chapter_entry.pack(anchor="w", padx=10, pady=(0, 5))
        
        ctk.CTkLabel(
            chapter_frame,
            text="This name will automatically replace {CHAPTER_NAME} in your prompts.",
            font=ctk.CTkFont(size=10),
            text_color="gray",
        ).pack(anchor="w", padx=10, pady=(0, 10))
        
        # JSON selection section
        json_frame = ctk.CTkFrame(main_frame)
        json_frame.pack(fill="x", pady=(0, 20))
        
        ctk.CTkLabel(
            json_frame,
            text="Input JSON (Stage 1 - final_output.json)",
            font=ctk.CTkFont(size=18, weight="bold"),
        ).pack(pady=(15, 10))
        
        file_frame = ctk.CTkFrame(json_frame)
        file_frame.pack(fill="x", padx=15, pady=5)
        
        ctk.CTkLabel(
            file_frame,
            text="Stage 1 JSON File:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        self.second_stage_json_var = ctk.StringVar()
        if self.last_final_output_path and os.path.exists(self.last_final_output_path):
            self.second_stage_json_var.set(self.last_final_output_path)
        
        json_entry = ctk.CTkEntry(file_frame, textvariable=self.second_stage_json_var, width=400)
        json_entry.pack(side="left", fill="x", expand=True, padx=(10, 5), pady=(0, 5))
        
        def browse_json_file():
            filename = filedialog.askopenfilename(
                title="Select Stage 1 final_output.json file",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            if filename:
                self.second_stage_json_var.set(filename)
        
        ctk.CTkButton(
            file_frame,
            text="Browse",
            command=browse_json_file,
            width=80,
        ).pack(side="right", padx=(5, 10), pady=(0, 5))
        
        # Model selection for second stage
        model_frame = ctk.CTkFrame(main_frame)
        model_frame.pack(fill="x", pady=(0, 20))
        
        ctk.CTkLabel(
            model_frame,
            text="Second-Stage Model Selection",
            font=ctk.CTkFont(size=18, weight="bold"),
        ).pack(pady=(15, 10))
        
        inner_model_frame = ctk.CTkFrame(model_frame)
        inner_model_frame.pack(fill="x", padx=15, pady=(0, 10))
        
        ctk.CTkLabel(
            inner_model_frame,
            text="Select model for second-stage (per-Part) processing:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        # Default to the model selected in the first page
        self.second_stage_model_var = ctk.StringVar(value=self.model_var.get())
        self.second_stage_model_combo = ctk.CTkComboBox(
            inner_model_frame,
            values=APIConfig.TEXT_MODELS,
            variable=self.second_stage_model_var,
            width=400,
        )
        self.second_stage_model_combo.pack(anchor="w", padx=10, pady=(0, 10))
        
        ctk.CTkLabel(
            inner_model_frame,
            text="You can use a lighter model (e.g., gemini-2.5-flash) for faster processing in the second stage.",
            font=ctk.CTkFont(size=10),
            text_color="gray",
        ).pack(anchor="w", padx=10, pady=(0, 5))
        
        #
        # --- Automatic Stage 3 & 4 settings (per-Part pipeline) ---
        #
        auto_frame = ctk.CTkFrame(main_frame)
        auto_frame.pack(fill="x", pady=(0, 20))
        
        ctk.CTkLabel(
            auto_frame,
            text="Automatic Pipeline Settings (Stages 3 & 4 per Part)",
            font=ctk.CTkFont(size=18, weight="bold"),
        ).pack(pady=(15, 10))
        
        # Stage 3 prompt
        s3_frame = ctk.CTkFrame(auto_frame)
        s3_frame.pack(fill="x", padx=15, pady=(5, 10))
        
        ctk.CTkLabel(
            s3_frame,
            text="Stage 3 Prompt (structuring & point extraction):",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        self.auto_stage3_prompt_text = ctk.CTkTextbox(s3_frame, height=140, font=self.farsi_text_font)
        self.auto_stage3_prompt_text.pack(fill="x", padx=10, pady=(0, 10))
        
        ctk.CTkLabel(
            s3_frame,
            text="You can use {CHAPTER_NAME} and {PART_NUMBER} placeholders in this prompt.",
            font=ctk.CTkFont(size=10),
            text_color="gray",
        ).pack(anchor="w", padx=10, pady=(0, 5))
        
        # Stage 4 prompt
        s4_frame = ctk.CTkFrame(auto_frame)
        s4_frame.pack(fill="x", padx=15, pady=(0, 10))
        
        ctk.CTkLabel(
            s4_frame,
            text="Stage 4 Prompt (optional, e.g. question generation / extra notes):",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(5, 5))
        
        self.auto_stage4_prompt_text = ctk.CTkTextbox(s4_frame, height=120, font=self.farsi_text_font)
        self.auto_stage4_prompt_text.pack(fill="x", padx=10, pady=(0, 10))
        
        ctk.CTkLabel(
            s4_frame,
            text="You can also use {CHAPTER_NAME} and {PART_NUMBER} here.",
            font=ctk.CTkFont(size=10),
            text_color="gray",
        ).pack(anchor="w", padx=10, pady=(0, 5))
        
        # Stage 3 / 4 model & PointId
        model34_frame = ctk.CTkFrame(auto_frame)
        model34_frame.pack(fill="x", padx=15, pady=(0, 10))
        
        # Stage 3 model
        s3_model_row = ctk.CTkFrame(model34_frame)
        s3_model_row.pack(fill="x", pady=(5, 5))
        
        ctk.CTkLabel(
            s3_model_row,
            text="Stage 3 Model:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(side="left", padx=(10, 5), pady=(5, 5))
        
        self.auto_stage3_model_var = ctk.StringVar(value=self.model_var.get())
        self.auto_stage3_model_combo = ctk.CTkComboBox(
            s3_model_row,
            values=APIConfig.TEXT_MODELS,
            variable=self.auto_stage3_model_var,
            width=280,
        )
        self.auto_stage3_model_combo.pack(side="left", padx=(0, 10), pady=(5, 5))
        
        # Stage 4 model
        s4_model_row = ctk.CTkFrame(model34_frame)
        s4_model_row.pack(fill="x", pady=(5, 5))
        
        ctk.CTkLabel(
            s4_model_row,
            text="Stage 4 Model:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(side="left", padx=(10, 5), pady=(5, 5))
        
        self.auto_stage4_model_var = ctk.StringVar(value=self.model_var.get())
        self.auto_stage4_model_combo = ctk.CTkComboBox(
            s4_model_row,
            values=APIConfig.TEXT_MODELS,
            variable=self.auto_stage4_model_var,
            width=280,
        )
        self.auto_stage4_model_combo.pack(side="left", padx=(0, 10), pady=(5, 5))
        
        # Start PointId
        pointid_row = ctk.CTkFrame(model34_frame)
        pointid_row.pack(fill="x", pady=(5, 5))
        
        ctk.CTkLabel(
            pointid_row,
            text="Start PointId (e.g. 1050030000):",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(5, 5))
        
        self.auto_start_pointid_var = ctk.StringVar(value="")
        ctk.CTkEntry(
            pointid_row,
            textvariable=self.auto_start_pointid_var,
            width=300,
        ).pack(anchor="w", padx=10, pady=(0, 5))
        
        ctk.CTkLabel(
            pointid_row,
            text="If empty, a default starting PointId will be used.",
            font=ctk.CTkFont(size=10),
            text_color="gray",
        ).pack(anchor="w", padx=10, pady=(0, 5))
        
        #
        # --- Progress & control buttons ---
        #
        progress_frame = ctk.CTkFrame(main_frame)
        progress_frame.pack(fill="x", pady=(10, 10))
        
        ctk.CTkLabel(
            progress_frame,
            text="Pipeline Progress (Stages 2 to 3 to 4 per Part)",
            font=ctk.CTkFont(size=14, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        self.pipeline_progress_bar = ctk.CTkProgressBar(progress_frame)
        self.pipeline_progress_bar.pack(fill="x", padx=10, pady=(0, 5))
        self.pipeline_progress_bar.set(0.0)
        
        self.pipeline_progress_label = ctk.CTkLabel(
            progress_frame,
            text="Waiting to start...",
            font=ctk.CTkFont(size=10),
            text_color="gray",
        )
        self.pipeline_progress_label.pack(anchor="w", padx=10, pady=(0, 5))
        
        # Control buttons
        controls_frame = ctk.CTkFrame(main_frame)
        controls_frame.pack(fill="x", pady=(10, 10))
        
        def start_second_stage_only():
            # فقط Stage 2 (پردازش پارت‌ها) – رفتار قبلی
            start_stage2_btn.configure(state="disabled", text="Processing Stage 2...")
            threading.Thread(
                target=self.process_json_by_parts_worker,
                args=(window, start_stage2_btn),
                daemon=True,
            ).start()
        
        start_stage2_btn = ctk.CTkButton(
            controls_frame,
            text="Run Stage 2 Only (per Part)",
            command=start_second_stage_only,
            width=240,
            height=40,
            font=ctk.CTkFont(size=14, weight="bold"),
        )
        start_stage2_btn.pack(side="left", padx=10, pady=10)
        
        # Full pipeline button
        self.full_pipeline_cancel = False
        
        def start_full_pipeline():
            full_btn.configure(state="disabled", text="Running Full Pipeline...")
            self.full_pipeline_cancel = False
            threading.Thread(
                target=self.process_full_pipeline_worker,
                args=(window, full_btn),
                daemon=True,
            ).start()
        
        full_btn = ctk.CTkButton(
            controls_frame,
            text="Run Full Pipeline (Stage 2 to 3 to 4 per Part)",
            command=start_full_pipeline,
            width=320,
            height=40,
            font=ctk.CTkFont(size=14, weight="bold"),
        )
        full_btn.pack(side="left", padx=10, pady=10)
        
        def cancel_pipeline():
            self.full_pipeline_cancel = True
            self.update_status("Cancellation requested. Current part will finish, then pipeline will stop.")
            self.pipeline_progress_label.configure(
                text="Cancellation requested...",
                text_color="orange",
            )
        
        cancel_btn = ctk.CTkButton(
            controls_frame,
            text="Cancel Pipeline",
            command=cancel_pipeline,
            width=160,
            height=40,
            font=ctk.CTkFont(size=13),
            fg_color="red",
            hover_color="darkred",
        )
        cancel_btn.pack(side="left", padx=10, pady=10)
        
        ctk.CTkButton(
            controls_frame,
            text="Close",
            command=window.destroy,
            width=100,
            height=40,
        ).pack(side="left", padx=10, pady=10)
    
    def setup_stages_2_3_4_ui(self, parent):
        """
        Setup UI for Stages 2-3-4 (same as open_part_processing_window but for tabview)
        This is the actual Stages 1-4 form that should be shown in tabview
        """
        # Check if this is the tabview version (not main view)
        is_tabview = hasattr(self, 'main_stages_1_4_frame') and parent != self.main_stages_1_4_frame
        
        main_frame = ctk.CTkScrollableFrame(parent)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Add navigation button only in tabview version
        if is_tabview:
            nav_frame = ctk.CTkFrame(main_frame)
            nav_frame.pack(fill="x", pady=(0, 10))
            ctk.CTkButton(
                nav_frame,
                text="< Back to Main View",
                command=self.show_main_view,
                width=150,
                height=30,
                font=ctk.CTkFont(size=12),
                fg_color="gray",
                hover_color="darkgray"
            ).pack(side="left", padx=10, pady=5)
        
        title = ctk.CTkLabel(
            main_frame,
            text="Second Stage & Automatic Pipeline (Stages 2 to 3 to 4)",
            font=ctk.CTkFont(size=22, weight="bold"),
        )
        title.pack(pady=(0, 20))
        
        #
        # --- Stage 2 configuration (per-Part processing, قبلی) ---
        #
        stage2_frame = ctk.CTkFrame(main_frame)
        stage2_frame.pack(fill="x", pady=(0, 20))
        
        ctk.CTkLabel(
            stage2_frame,
            text="Second-Stage Prompt (per Part)",
            font=ctk.CTkFont(size=18, weight="bold"),
        ).pack(pady=(15, 10))
        
        ctk.CTkLabel(
            stage2_frame,
            text="This prompt will be applied separately to each Part in Stage 2.\n"
                 "The JSON rows for each Part will be sent to the model with this prompt.",
            font=ctk.CTkFont(size=11),
            text_color="gray",
        ).pack(anchor="w", padx=10, pady=(0, 10))
        
        # Only create if doesn't exist
        if not hasattr(self, 'second_stage_prompt_text'):
            self.second_stage_prompt_text = ctk.CTkTextbox(stage2_frame, height=140, font=self.farsi_text_font)
            self.second_stage_prompt_text.pack(fill="x", padx=10, pady=(0, 10))
        else:
            # If exists, repack it in the new location
            try:
                self.second_stage_prompt_text.pack_forget()
            except:
                pass
            self.second_stage_prompt_text.pack(fill="x", padx=10, pady=(0, 10))
        
        # Pre-fill with current prompt if available
        current_prompt = self.get_selected_prompt()
        if current_prompt and not self.second_stage_prompt_text.get("1.0", tk.END).strip():
            self.second_stage_prompt_text.insert("1.0", current_prompt)
        
        # Chapter name section (right after prompt)
        chapter_frame = ctk.CTkFrame(stage2_frame)
        chapter_frame.pack(fill="x", padx=10, pady=(0, 10))
        
        ctk.CTkLabel(
            chapter_frame,
            text="Chapter Name:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        # Only create if doesn't exist
        if not hasattr(self, 'second_stage_chapter_var'):
            self.second_stage_chapter_var = ctk.StringVar(value="درمان با UV")
        chapter_entry = ctk.CTkEntry(
            chapter_frame,
            textvariable=self.second_stage_chapter_var,
            width=400
        )
        chapter_entry.pack(anchor="w", padx=10, pady=(0, 5))
        
        ctk.CTkLabel(
            chapter_frame,
            text="This name will automatically replace {CHAPTER_NAME} in your prompts.",
            font=ctk.CTkFont(size=10),
            text_color="gray",
        ).pack(anchor="w", padx=10, pady=(0, 10))
        
        # JSON selection section
        json_frame = ctk.CTkFrame(main_frame)
        json_frame.pack(fill="x", pady=(0, 20))
        
        ctk.CTkLabel(
            json_frame,
            text="Input JSON (Stage 1 - final_output.json)",
            font=ctk.CTkFont(size=18, weight="bold"),
        ).pack(pady=(15, 10))
        
        file_frame = ctk.CTkFrame(json_frame)
        file_frame.pack(fill="x", padx=15, pady=5)
        
        ctk.CTkLabel(
            file_frame,
            text="Stage 1 JSON File:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        # Only create if doesn't exist
        if not hasattr(self, 'second_stage_json_var'):
            self.second_stage_json_var = ctk.StringVar()
            if self.last_final_output_path and os.path.exists(self.last_final_output_path):
                self.second_stage_json_var.set(self.last_final_output_path)
        
        json_entry = ctk.CTkEntry(file_frame, textvariable=self.second_stage_json_var, width=400)
        json_entry.pack(side="left", fill="x", expand=True, padx=(10, 5), pady=(0, 5))
        
        def browse_json_file():
            filename = filedialog.askopenfilename(
                title="Select Stage 1 final_output.json file",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            if filename:
                self.second_stage_json_var.set(filename)
        
        ctk.CTkButton(
            file_frame,
            text="Browse",
            command=browse_json_file,
            width=80,
        ).pack(side="right", padx=(5, 10), pady=(0, 5))
        
        # Model selection for second stage
        model_frame = ctk.CTkFrame(main_frame)
        model_frame.pack(fill="x", pady=(0, 20))
        
        ctk.CTkLabel(
            model_frame,
            text="Second-Stage Model Selection",
            font=ctk.CTkFont(size=18, weight="bold"),
        ).pack(pady=(15, 10))
        
        inner_model_frame = ctk.CTkFrame(model_frame)
        inner_model_frame.pack(fill="x", padx=15, pady=(0, 10))
        
        ctk.CTkLabel(
            inner_model_frame,
            text="Select model for second-stage (per-Part) processing:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        # Only create if doesn't exist
        if not hasattr(self, 'second_stage_model_var'):
            self.second_stage_model_var = ctk.StringVar(value=self.model_var.get() if hasattr(self, 'model_var') else APIConfig.TEXT_MODELS[0])
        if not hasattr(self, 'second_stage_model_combo'):
            self.second_stage_model_combo = ctk.CTkComboBox(
                inner_model_frame,
                values=APIConfig.TEXT_MODELS,
                variable=self.second_stage_model_var,
                width=400,
            )
            self.second_stage_model_combo.pack(anchor="w", padx=10, pady=(0, 10))
        else:
            # If exists, repack it in the new location
            try:
                self.second_stage_model_combo.pack_forget()
            except:
                pass
            self.second_stage_model_combo.pack(anchor="w", padx=10, pady=(0, 10))
        
        ctk.CTkLabel(
            inner_model_frame,
            text="You can use a lighter model (e.g., gemini-2.5-flash) for faster processing in the second stage.",
            font=ctk.CTkFont(size=10),
            text_color="gray",
        ).pack(anchor="w", padx=10, pady=(0, 5))
        
        #
        # --- Automatic Stage 3 & 4 settings (per-Part pipeline) ---
        #
        auto_frame = ctk.CTkFrame(main_frame)
        auto_frame.pack(fill="x", pady=(0, 20))
        
        ctk.CTkLabel(
            auto_frame,
            text="Automatic Pipeline Settings (Stages 3 & 4 per Part)",
            font=ctk.CTkFont(size=18, weight="bold"),
        ).pack(pady=(15, 10))
        
        # Stage 3 prompt
        s3_frame = ctk.CTkFrame(auto_frame)
        s3_frame.pack(fill="x", padx=15, pady=(5, 10))
        
        ctk.CTkLabel(
            s3_frame,
            text="Stage 3 Prompt (structuring & point extraction):",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        # Only create if doesn't exist
        if not hasattr(self, 'auto_stage3_prompt_text'):
            self.auto_stage3_prompt_text = ctk.CTkTextbox(s3_frame, height=140, font=self.farsi_text_font)
        self.auto_stage3_prompt_text.pack(fill="x", padx=10, pady=(0, 10))
        
        ctk.CTkLabel(
            s3_frame,
            text="You can use {CHAPTER_NAME} and {PART_NUMBER} placeholders in this prompt.",
            font=ctk.CTkFont(size=10),
            text_color="gray",
        ).pack(anchor="w", padx=10, pady=(0, 5))
        
        # Stage 4 prompt
        s4_frame = ctk.CTkFrame(auto_frame)
        s4_frame.pack(fill="x", padx=15, pady=(0, 10))
        
        ctk.CTkLabel(
            s4_frame,
            text="Stage 4 Prompt (optional, e.g. question generation / extra notes):",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(5, 5))
        
        # Only create if doesn't exist
        if not hasattr(self, 'auto_stage4_prompt_text'):
            self.auto_stage4_prompt_text = ctk.CTkTextbox(s4_frame, height=120, font=self.farsi_text_font)
            self.auto_stage4_prompt_text.pack(fill="x", padx=10, pady=(0, 10))
        else:
            # If exists, repack it in the new location
            try:
                self.auto_stage4_prompt_text.pack_forget()
            except:
                pass
            self.auto_stage4_prompt_text.pack(fill="x", padx=10, pady=(0, 10))
        
        ctk.CTkLabel(
            s4_frame,
            text="You can also use {CHAPTER_NAME} and {PART_NUMBER} here.",
            font=ctk.CTkFont(size=10),
            text_color="gray",
        ).pack(anchor="w", padx=10, pady=(0, 5))
        
        # Stage 3 / 4 model & PointId
        model34_frame = ctk.CTkFrame(auto_frame)
        model34_frame.pack(fill="x", padx=15, pady=(0, 10))
        
        # Stage 3 model
        s3_model_row = ctk.CTkFrame(model34_frame)
        s3_model_row.pack(fill="x", pady=(5, 5))
        
        ctk.CTkLabel(
            s3_model_row,
            text="Stage 3 Model:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(side="left", padx=(10, 5), pady=(5, 5))
        
        # Only create if doesn't exist
        if not hasattr(self, 'auto_stage3_model_var'):
            self.auto_stage3_model_var = ctk.StringVar(value=self.model_var.get() if hasattr(self, 'model_var') else APIConfig.TEXT_MODELS[0])
        if not hasattr(self, 'auto_stage3_model_combo'):
            self.auto_stage3_model_combo = ctk.CTkComboBox(
                s3_model_row,
                values=APIConfig.TEXT_MODELS,
                variable=self.auto_stage3_model_var,
                width=280,
            )
            self.auto_stage3_model_combo.pack(side="left", padx=(0, 10), pady=(5, 5))
        else:
            # If exists, repack it in the new location
            try:
                self.auto_stage3_model_combo.pack_forget()
            except:
                pass
            self.auto_stage3_model_combo.pack(side="left", padx=(0, 10), pady=(5, 5))
        
        # Stage 4 model
        s4_model_row = ctk.CTkFrame(model34_frame)
        s4_model_row.pack(fill="x", pady=(5, 5))
        
        ctk.CTkLabel(
            s4_model_row,
            text="Stage 4 Model:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(side="left", padx=(10, 5), pady=(5, 5))
        
        # Only create if doesn't exist
        if not hasattr(self, 'auto_stage4_model_var'):
            self.auto_stage4_model_var = ctk.StringVar(value=self.model_var.get() if hasattr(self, 'model_var') else APIConfig.TEXT_MODELS[0])
        if not hasattr(self, 'auto_stage4_model_combo'):
            self.auto_stage4_model_combo = ctk.CTkComboBox(
                s4_model_row,
                values=APIConfig.TEXT_MODELS,
                variable=self.auto_stage4_model_var,
                width=280,
            )
            self.auto_stage4_model_combo.pack(side="left", padx=(0, 10), pady=(5, 5))
        else:
            # If exists, repack it in the new location
            try:
                self.auto_stage4_model_combo.pack_forget()
            except:
                pass
            self.auto_stage4_model_combo.pack(side="left", padx=(0, 10), pady=(5, 5))
        
        # Start PointId
        pointid_row = ctk.CTkFrame(model34_frame)
        pointid_row.pack(fill="x", pady=(5, 5))
        
        ctk.CTkLabel(
            pointid_row,
            text="Start PointId (e.g. 1050030000):",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(5, 5))
        
        # Only create if doesn't exist
        if not hasattr(self, 'auto_start_pointid_var'):
            self.auto_start_pointid_var = ctk.StringVar(value="")
        ctk.CTkEntry(
            pointid_row,
            textvariable=self.auto_start_pointid_var,
            width=300,
        ).pack(anchor="w", padx=10, pady=(0, 5))
        
        ctk.CTkLabel(
            pointid_row,
            text="If empty, a default starting PointId will be used.",
            font=ctk.CTkFont(size=10),
            text_color="gray",
        ).pack(anchor="w", padx=10, pady=(0, 5))
        
        #
        # --- Progress & control buttons ---
        #
        progress_frame = ctk.CTkFrame(main_frame)
        progress_frame.pack(fill="x", pady=(10, 10))
        
        ctk.CTkLabel(
            progress_frame,
            text="Pipeline Progress (Stages 2 to 3 to 4 per Part)",
            font=ctk.CTkFont(size=14, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        # Only create if doesn't exist
        if not hasattr(self, 'pipeline_progress_bar'):
            self.pipeline_progress_bar = ctk.CTkProgressBar(progress_frame)
        self.pipeline_progress_bar.pack(fill="x", padx=10, pady=(0, 5))
        self.pipeline_progress_bar.set(0.0)
        
        if not hasattr(self, 'pipeline_progress_label'):
            self.pipeline_progress_label = ctk.CTkLabel(
                progress_frame,
                text="Waiting to start...",
                font=ctk.CTkFont(size=10),
                text_color="gray",
            )
        self.pipeline_progress_label.pack(anchor="w", padx=10, pady=(0, 5))
        
        # Control buttons
        controls_frame = ctk.CTkFrame(main_frame)
        controls_frame.pack(fill="x", pady=(10, 10))
        
        def start_second_stage_only():
            # فقط Stage 2 (پردازش پارت‌ها) – رفتار قبلی
            start_stage2_btn.configure(state="disabled", text="Processing Stage 2...")
            threading.Thread(
                target=self.process_json_by_parts_worker,
                args=(self.root, start_stage2_btn),
                daemon=True,
            ).start()
        
        start_stage2_btn = ctk.CTkButton(
            controls_frame,
            text="Run Stage 2 Only (per Part)",
            command=start_second_stage_only,
            width=240,
            height=40,
            font=ctk.CTkFont(size=14, weight="bold"),
        )
        start_stage2_btn.pack(side="left", padx=10, pady=10)
        
        # Full pipeline button
        if not hasattr(self, 'full_pipeline_cancel'):
            self.full_pipeline_cancel = False
        
        def start_full_pipeline():
            full_btn.configure(state="disabled", text="Running Full Pipeline...")
            self.full_pipeline_cancel = False
            threading.Thread(
                target=self.process_full_pipeline_worker,
                args=(self.root, full_btn),
                daemon=True,
            ).start()
        
        full_btn = ctk.CTkButton(
            controls_frame,
            text="Run Full Pipeline (Stage 2 to 3 to 4 per Part)",
            command=start_full_pipeline,
            width=320,
            height=40,
            font=ctk.CTkFont(size=14, weight="bold"),
        )
        full_btn.pack(side="left", padx=10, pady=10)
        
        def cancel_pipeline():
            self.full_pipeline_cancel = True
            self.update_status("Cancellation requested. Current part will finish, then pipeline will stop.")
            self.pipeline_progress_label.configure(
                text="Cancellation requested...",
                text_color="orange",
            )
        
        cancel_btn = ctk.CTkButton(
            controls_frame,
            text="Cancel Pipeline",
            command=cancel_pipeline,
            width=160,
            height=40,
            font=ctk.CTkFont(size=13),
            fg_color="red",
            hover_color="darkred",
        )
        cancel_btn.pack(side="left", padx=10, pady=10)

    def process_json_by_parts_worker(self, parent_window, start_button):
        """
        Background worker to process an existing final_output.json part-by-part
        using the second-stage prompt.
        """
        try:
            prompt = self.second_stage_prompt_text.get("1.0", tk.END).strip()
            if not prompt:
                messagebox.showerror("Error", "Please enter a prompt for second-stage processing.")
                return

            # Get chapter name and replace placeholder in prompt
            chapter_name = self.second_stage_chapter_var.get().strip()
            if "{CHAPTER_NAME}" in prompt:
                prompt = prompt.replace("{CHAPTER_NAME}", chapter_name)

            json_path = self.second_stage_json_var.get().strip()
            if not json_path:
                if self.last_final_output_path and os.path.exists(self.last_final_output_path):
                    json_path = self.last_final_output_path
                    self.second_stage_json_var.set(json_path)
                else:
                    messagebox.showerror("Error", "Please select the input JSON file.")
                    return

            if not os.path.exists(json_path):
                messagebox.showerror("Error", f"JSON file not found:\n{json_path}")
                return

            # Use selected model for second stage
            model_name = self.second_stage_model_var.get()

            self.update_status("Starting second-stage part processing...")

            final_path = self.multi_part_post_processor.process_final_json_by_parts(
                json_path=json_path,
                user_prompt=prompt,
                model_name=model_name,
            )

            if not final_path or not os.path.exists(final_path):
                self.update_status("Second-stage part processing failed.")
                messagebox.showerror("Error", "Second-stage part processing failed. Check logs for details.")
                return

            # Load and display final output (JSON if available, otherwise text)
            try:
                if final_path.endswith('.json'):
                    # Load JSON and display
                    with open(final_path, "r", encoding="utf-8") as f:
                        final_data = json.load(f)
                    
                    self.update_status("Second-stage processing completed.")
                    
                    # Store post-processed path for third stage
                    self.last_post_processed_path = final_path
                    
                    # Display as formatted JSON
                    response_text = json.dumps(final_data, ensure_ascii=False, indent=2)
                    self.show_response_window(response_text, final_path, False, True)
                else:
                    # Load as text
                    with open(final_path, "r", encoding="utf-8") as f:
                        response_text = f.read()
                    
                    self.update_status("Second-stage processing completed.")
                    
                    # Store post-processed path for third stage
                    self.last_post_processed_path = final_path
                    
                    # Show raw response text as-is
                    self.show_response_window(response_text, final_path, False, False)
            except Exception as e:
                self.update_status(f"Error loading post-processed output: {str(e)}")
                messagebox.showerror("Error", f"Failed to load post-processed output:\n{str(e)}")
                return

        except Exception as e:
            self.logger.error(f"Error in second-stage processing: {str(e)}", exc_info=True)
            messagebox.showerror("Error", f"Second-stage processing error:\n{str(e)}")
        finally:
            # Re-enable start button on UI thread
            try:
                self.root.after(
                    0,
                    lambda: start_button.configure(state="normal", text="Run Stage 2 Only (per Part)")
                )
            except Exception:
                pass

    def process_full_pipeline_worker(self, parent_window, start_button):
        """
        Background worker: run Stage 2 → 3 → 4 for all Parts automatically.
        Uses:
          - Stage 1 JSON (final_output.json)
          - Stage 2 per-Part prompt/model
          - Stage 3 / 4 prompts and models
          - Start PointId for the whole chapter
        """
        try:
            # --- Validate and load basic inputs ---
            stage1_path = self.second_stage_json_var.get().strip()
            if not stage1_path:
                if self.last_final_output_path and os.path.exists(self.last_final_output_path):
                    stage1_path = self.last_final_output_path
                    self.second_stage_json_var.set(stage1_path)
                else:
                    messagebox.showerror("Error", "Please select the Stage 1 JSON file (final_output.json).")
                    return

            if not os.path.exists(stage1_path):
                messagebox.showerror("Error", f"Stage 1 JSON file not found:\n{stage1_path}")
                return

            stage2_prompt = self.second_stage_prompt_text.get("1.0", tk.END).strip()
            if not stage2_prompt:
                messagebox.showerror("Error", "Please enter a prompt for second-stage (per-Part) processing.")
                return

            chapter_name = self.second_stage_chapter_var.get().strip()
            if "{CHAPTER_NAME}" in stage2_prompt and chapter_name:
                stage2_prompt = stage2_prompt.replace("{CHAPTER_NAME}", chapter_name)

            stage2_model = self.second_stage_model_var.get().strip()
            if not stage2_model:
                stage2_model = self.model_var.get()

            # Stage 3 / 4 prompts
            stage3_prompt = self.auto_stage3_prompt_text.get("1.0", tk.END).strip()
            if not stage3_prompt:
                messagebox.showerror("Error", "Please enter Stage 3 prompt (structuring & point extraction).")
                return

            stage4_prompt = self.auto_stage4_prompt_text.get("1.0", tk.END).strip()

            stage3_model = self.auto_stage3_model_var.get().strip() or self.model_var.get()
            stage4_model = self.auto_stage4_model_var.get().strip() or self.model_var.get()

            # PointId handling
            start_pointid_str = self.auto_start_pointid_var.get().strip()
            if not start_pointid_str:
                # Default for single-chapter mode (user can change later)
                start_pointid_str = "1050030000"

            if not (start_pointid_str.isdigit() and len(start_pointid_str) == 10):
                messagebox.showerror("Error", "Start PointId must be a 10-digit number, e.g. 1050030000.")
                return

            try:
                book_id = int(start_pointid_str[0:3])
                chapter_id_num = int(start_pointid_str[3:6])
                current_index = int(start_pointid_str[6:10])
            except ValueError:
                messagebox.showerror("Error", "Start PointId format is invalid.")
                return

            self.update_status("Loading Stage 1 JSON (final_output)...")
            try:
                with open(stage1_path, "r", encoding="utf-8") as f:
                    stage1_data = json.load(f)
            except Exception as e:
                messagebox.showerror("Error", f"Failed to load Stage 1 JSON:\n{str(e)}")
                return

            rows = stage1_data.get("rows", [])
            if not isinstance(rows, list) or not rows:
                messagebox.showerror("Error", "Stage 1 JSON has no 'rows' to process.")
                return

            # Discover Parts from Stage 1
            parts = {}
            for row in rows:
                if not isinstance(row, dict):
                    continue
                part_value = row.get("Part", 0)
                try:
                    part_num = int(part_value) if part_value not in (None, "") else 0
                except (ValueError, TypeError):
                    part_num = 0
                parts.setdefault(part_num, []).append(row)

            if not parts:
                messagebox.showerror("Error", "No valid Part information found in Stage 1 JSON.")
                return

            sorted_parts = sorted(parts.keys())
            total_parts = len(sorted_parts)

            # --- Stage 2: per-Part processing using existing MultiPartPostProcessor ---
            # We reuse existing Stage 2 module which already does per-Part calls and returns a single JSON.
            # Now we also get individual responses per Part for use in Stage 3.
            self.update_status("Running Stage 2 (per-Part) processing for all parts...")
            stage2_path, stage2_part_responses = self.multi_part_post_processor.process_final_json_by_parts_with_responses(
                json_path=stage1_path,
                user_prompt=stage2_prompt,
                model_name=stage2_model,
            )

            if not stage2_path or not os.path.exists(stage2_path):
                self.update_status("Stage 2 processing failed.")
                messagebox.showerror("Error", "Stage 2 processing failed. Check logs for details.")
                return

            if not stage2_part_responses:
                self.update_status("Stage 2 processing failed: No individual responses received.")
                messagebox.showerror("Error", "Stage 2 processing failed: No individual responses received.")
                return

            self.last_post_processed_path = stage2_path

            try:
                with open(stage2_path, "r", encoding="utf-8") as f:
                    stage2_data = json.load(f)
            except Exception as e:
                messagebox.showerror("Error", f"Failed to load Stage 2 JSON:\n{str(e)}")
                return

            # Stage 2: Each Part has its own response. For Stage 3, we use the response
            # from the same Part (not the combined JSON). This ensures each Part's
            # Stage 3 processing uses its corresponding Stage 2 response.

            processed_parts = 0
            stage3_txt_files = []
            stage4_txt_files = []
            all_stage4_rows: List[Dict[str, Any]] = []

            def _update_progress(part_idx: int, stage_label: str):
                if total_parts <= 0:
                    self.pipeline_progress_bar.set(0.0)
                    self.pipeline_progress_label.configure(text="No parts to process.", text_color="gray")
                    return
                frac = min(max(part_idx / total_parts, 0.0), 1.0)
                self.pipeline_progress_bar.set(frac)
                self.pipeline_progress_label.configure(
                    text=f"{stage_label} - Part {part_idx}/{total_parts}",
                    text_color="white",
                )

            # Loop over Parts
            for idx, part_num in enumerate(sorted_parts, start=1):
                if self.full_pipeline_cancel:
                    self.update_status(f"Pipeline cancelled by user. Stopped at Part {idx}/{total_parts}.")
                    break

                part_rows = parts.get(part_num) or []
                if not part_rows:
                    continue

                # Restrict Stage 1 JSON to this Part only
                stage1_part = {
                    "metadata": stage1_data.get("metadata", {}),
                    "rows": part_rows,
                }

                # Stage 3: برای هر پارت فقط یک بار مدل را صدا می‌زنیم و هیچ تبدیل JSON انجام نمی‌دهیم؛
                # فقط پاسخ خام مدل را به‌صورت فایل متنی ذخیره می‌کنیم.
                # از Response Stage 2 همان Part استفاده می‌کنیم (نه combined JSON).

                _update_progress(idx - 1, "Stage 3 (structuring)")
                self.update_status(f"Stage 3: Processing Part {part_num} ({idx}/{total_parts})...")

                # Build Stage 3 prompt (بدون وابستگی به شماره پارت / چانک)
                s3_prompt_part = stage3_prompt
                if "{CHAPTER_NAME}" in s3_prompt_part and chapter_name:
                    s3_prompt_part = s3_prompt_part.replace("{CHAPTER_NAME}", chapter_name)

                # Get Stage 2 response for this specific Part
                stage2_response_for_part = stage2_part_responses.get(part_num, "")
                if not stage2_response_for_part:
                    self.update_status(
                        f"Warning: No Stage 2 response found for Part {part_num}. Skipping Stage 3 for this Part."
                    )
                    continue

                try:
                    s1_str = json.dumps(stage1_part, ensure_ascii=False, indent=2)
                except Exception as e:
                    self.update_status(
                        f"Error serializing JSON for Stage 3 (Part {part_num}): {e}"
                    )
                    messagebox.showerror(
                        "Error",
                        f"Error serializing JSON for Stage 3 (Part {part_num}):\n{str(e)}",
                    )
                    return

                s3_full_prompt = (
                    f"{s3_prompt_part}\n\n"
                    "====================\n"
                    "Stage 1 JSON (this Part only):\n"
                    "====================\n"
                    f"{s1_str}\n\n"
                    "====================\n"
                    "Stage 2 Response (for this Part):\n"
                    "====================\n"
                    f"{stage2_response_for_part}\n"
                )

                # مشابه Stage 2: فقط متن خام پاسخ را می‌گیریم (در صورت نیاز با چند ری‌تری) و به‌صورت txt ذخیره می‌کنیم.
                s3_response_text = None
                max_s3_retries = 2

                for attempt in range(1, max_s3_retries + 2):  # تلاش اولیه + حداکثر دو ری‌تری
                    if self.full_pipeline_cancel:
                        self.update_status(
                            f"Pipeline cancelled by user during Stage 3. Stopped at Part {idx}/{total_parts}."
                        )
                        return

                    if attempt > 1:
                        self.update_status(
                            f"Stage 3: Retrying Part {part_num} (attempt {attempt}/{max_s3_retries + 1})..."
                        )

                    s3_response_text = self.api_client.process_text(
                        text=s3_full_prompt,
                        system_prompt=None,
                        model_name=stage3_model,
                        temperature=APIConfig.DEFAULT_TEMPERATURE,
                        max_tokens=APIConfig.DEFAULT_MAX_TOKENS,
                    )

                    if s3_response_text:
                        break

                if not s3_response_text:
                    self.update_status(
                        f"Stage 3 failed for Part {part_num} (no response from model after retries). Skipping this Part."
                    )
                    continue

                # ذخیره خروجی Stage 3 این پارت به‌صورت فایل متنی
                s3_txt_path = None
                try:
                    base_dir = os.path.dirname(stage1_path) or os.getcwd()
                    base_name, _ = os.path.splitext(os.path.basename(stage1_path))
                    s3_txt_name = f"{base_name}_part{part_num}_stage3.txt"
                    s3_txt_path = os.path.join(base_dir, s3_txt_name)
                    with open(s3_txt_path, "w", encoding="utf-8") as f:
                        f.write(s3_response_text)
                    self.update_status(f"Stage 3 raw text saved for Part {part_num}: {s3_txt_path}")
                    stage3_txt_files.append(s3_txt_path)
                except Exception as e:
                    self.update_status(
                        f"Error saving Stage 3 text output for Part {part_num}: {e}"
                    )

                # Stage 4 برای این پارت: ورودی فقط پرامپت + خروجی Stage 3
                if stage4_prompt:
                    if self.full_pipeline_cancel:
                        self.update_status(f"Pipeline cancelled by user during Stage 4. Stopped at Part {idx}/{total_parts}.")
                        break

                    _update_progress(idx - 1, "Stage 4 (extra points)")
                    self.update_status(f"Stage 4: Processing Part {part_num} ({idx}/{total_parts})...")

                    s4_prompt_part = stage4_prompt
                    if "{CHAPTER_NAME}" in s4_prompt_part and chapter_name:
                        s4_prompt_part = s4_prompt_part.replace("{CHAPTER_NAME}", chapter_name)

                    # آماده‌سازی Stage 1 JSON برای همین پارت
                    try:
                        s1_str_4 = json.dumps(stage1_part, ensure_ascii=False, indent=2)
                    except Exception as e:
                        self.update_status(
                            f"Error serializing Stage 1 JSON for Stage 4 (Part {part_num}): {e}"
                        )
                        messagebox.showerror(
                            "Error",
                            f"Error serializing Stage 1 JSON for Stage 4 (Part {part_num}):\n{str(e)}",
                        )
                        return

                    # خواندن خروجی Stage 3 از فایل txt
                    s3_output_text = ""
                    if s3_txt_path and os.path.exists(s3_txt_path):
                        try:
                            with open(s3_txt_path, "r", encoding="utf-8") as f:
                                s3_output_text = f.read()
                        except Exception as e:
                            self.update_status(
                                f"Warning: Failed to read Stage 3 output for Part {part_num}: {e}"
                            )
                    else:
                        # اگر فایل Stage 3 ذخیره نشده، از پاسخ خام استفاده می‌کنیم
                        s3_output_text = s3_response_text

                    if not s3_output_text:
                        self.update_status(
                            f"Warning: No Stage 3 output available for Part {part_num}. Skipping Stage 4."
                        )
                        continue

                    # ساخت prompt Stage 4: پرامپت + Stage 1 JSON + خروجی Stage 3
                    s4_full_prompt = (
                        f"{s4_prompt_part}\n\n"
                        "====================\n"
                        "Stage 1 JSON (this Part only):\n"
                        "====================\n"
                        f"{s1_str_4}\n\n"
                        "====================\n"
                        "Stage 3 output for this Part:\n"
                        "====================\n"
                        f"{s3_output_text}\n"
                    )

                    s4_response_text = None
                    max_s4_retries = 2

                    for attempt in range(1, max_s4_retries + 2):
                        if self.full_pipeline_cancel:
                            self.update_status(
                                f"Pipeline cancelled by user during Stage 4. Stopped at Part {idx}/{total_parts}."
                            )
                            break

                        if attempt > 1:
                            self.update_status(
                                f"Stage 4: Retrying Part {part_num} (attempt {attempt}/{max_s4_retries + 1})..."
                            )

                        s4_response_text = self.api_client.process_text(
                            text=s4_full_prompt,
                            system_prompt=None,
                            model_name=stage4_model,
                            temperature=APIConfig.DEFAULT_TEMPERATURE,
                            max_tokens=APIConfig.DEFAULT_MAX_TOKENS,
                        )

                        if not s4_response_text:
                            self.update_status(
                                f"Stage 4 failed for Part {part_num} (no response, attempt {attempt})."
                            )
                            if attempt > max_s4_retries:
                                self.update_status(
                                    f"Warning: Skipping Part {part_num} in Stage 4 after repeated no-response."
                                )
                            continue

                        if s4_response_text:
                            break

                    if not s4_response_text:
                        self.update_status(
                            f"Warning: No valid text response in Stage 4 for Part {part_num} after retries."
                        )
                    else:
                        # ذخیره خروجی Stage 4 این پارت به‌صورت فایل متنی
                        try:
                            base_dir = os.path.dirname(stage1_path) or os.getcwd()
                            base_name, _ = os.path.splitext(os.path.basename(stage1_path))
                            s4_txt_name = f"{base_name}_part{part_num}_stage4.txt"
                            s4_txt_path = os.path.join(base_dir, s4_txt_name)
                            with open(s4_txt_path, "w", encoding="utf-8") as f:
                                f.write(s4_response_text)
                            self.update_status(
                                f"Stage 4 raw text saved for Part {part_num}: {s4_txt_path}"
                            )
                            stage4_txt_files.append(s4_txt_path)
                        except Exception as e:
                            self.update_status(
                                f"Error saving Stage 4 text output for Part {part_num}: {e}"
                            )

                processed_parts += 1
                _update_progress(processed_parts, "Completed")

            # --- After all parts: build final JSON with flattened points and PointId ---
            self.update_status("Building final JSON with PointId from Stage 4 TXT files...")

            # Sort Stage 4 files by Part number to ensure correct order
            def extract_part_number(file_path: str) -> int:
                """Extract part number from filename like '*_part3_stage4.txt'"""
                import re
                match = re.search(r'_part(\d+)_stage4\.txt', os.path.basename(file_path))
                if match:
                    return int(match.group(1))
                return 0  # Default to 0 if not found
            
            sorted_stage4_files = sorted(stage4_txt_files, key=extract_part_number)
            self.logger.info(f"Processing {len(sorted_stage4_files)} Stage 4 files in order: {[extract_part_number(f) for f in sorted_stage4_files]}")

            # Convert each Stage 4 TXT to JSON, flatten, and accumulate rows
            converter = ThirdStageConverter()

            for s4_txt_path in sorted_stage4_files:
                part_num = extract_part_number(s4_txt_path)
                self.update_status(f"Processing Stage 4 file for Part {part_num}: {os.path.basename(s4_txt_path)}")
                
                json_obj = load_stage_txt_as_json(s4_txt_path)
                if not json_obj:
                    self.logger.warning(
                        "Skipping Stage 4 TXT (could not extract JSON): %s", s4_txt_path
                    )
                    continue

                try:
                    flat_rows = converter._flatten_to_points(json_obj)
                    self.logger.info(f"Part {part_num}: Extracted {len(flat_rows)} points from Stage 4")
                    all_stage4_rows.extend(flat_rows)
                except Exception as e:
                    self.logger.error(
                        "Failed to flatten Stage 4 JSON for %s: %s", s4_txt_path, e
                    )
                    continue

            if not all_stage4_rows:
                self.update_status("No valid points extracted from Stage 4 outputs.")
                messagebox.showerror(
                    "Error",
                    "No valid JSON/points could be extracted from Stage 4 TXT files.\n"
                    "Please check the logs and model outputs.",
                )
                return

            # Log summary of extracted points
            self.logger.info(f"Total {len(all_stage4_rows)} points extracted from {len(sorted_stage4_files)} Stage 4 files")
            self.update_status(f"Extracted {len(all_stage4_rows)} points from {len(sorted_stage4_files)} Stage 4 files")

            # Assign PointId sequentially across all parts, starting from user-provided index
            total_points = len(all_stage4_rows)
            assigned_points: List[Dict[str, Any]] = []

            self.update_status(f"Assigning PointId to {total_points} points (starting from {start_pointid_str})...")
            for idx, row in enumerate(all_stage4_rows, 1):
                point_id = f"{book_id:03d}{chapter_id_num:03d}{current_index:04d}"
                row["PointId"] = point_id
                current_index += 1
                assigned_points.append(row)
                if idx % 100 == 0:
                    self.update_status(f"Assigned PointId to {idx}/{total_points} points...")
            
            self.logger.info(f"Assigned PointId to {len(assigned_points)} points (next free index: {current_index})")
            self.update_status(f"Assigned PointId to all {len(assigned_points)} points")

            # --- Build final merged JSON with metadata + points ---
            base_dir = os.path.dirname(stage1_path) or os.getcwd()
            base_name, _ = os.path.splitext(os.path.basename(stage1_path))
            merged_filename = f"{base_name}_final_points.json"
            merged_path = os.path.join(base_dir, merged_filename)

            metadata = {
                "chapter": chapter_name,
                "book_id": book_id,
                "chapter_id": chapter_id_num,
                "processed_parts": processed_parts,
                "total_points": total_points,
                "start_point_index": int(start_pointid_str[6:10]),
                "next_free_index": current_index,
                "processed_at": datetime.now().isoformat(),
                "source_stage1": os.path.basename(stage1_path),
                "source_stage2": os.path.basename(stage2_path),
                "model_stage3": stage3_model,
                "model_stage4": stage4_model if stage4_prompt else None,
                "stage3_text_files": stage3_txt_files,
                "stage4_text_files": stage4_txt_files,
            }

            merged_data = {
                "metadata": metadata,
                "points": assigned_points,
            }

            try:
                with open(merged_path, "w", encoding="utf-8") as f:
                    json.dump(merged_data, f, ensure_ascii=False, indent=2)
            except Exception as e:
                self.logger.error(f"Failed to save merged pipeline JSON: {e}", exc_info=True)
                messagebox.showerror("Error", f"Failed to save merged pipeline JSON:\n{str(e)}")
                return

            self.last_corrected_path = merged_path
            self.update_status(f"Full pipeline completed. Merged JSON saved to: {merged_path}")
            self.pipeline_progress_bar.set(1.0)
            self.pipeline_progress_label.configure(
                text=f"Completed {processed_parts}/{total_parts} parts.",
                text_color="green",
            )

            # Show final result to user
            try:
                response_text = json.dumps(merged_data, ensure_ascii=False, indent=2)
                self.show_response_window(response_text, merged_path, False, True)
                messagebox.showinfo(
                    "Success",
                    f"Full pipeline completed.\n\nMerged JSON saved to:\n{merged_path}",
                )
            except Exception as e:
                self.logger.error(f"Failed to display merged pipeline JSON: {e}", exc_info=True)

        except Exception as e:
            self.logger.error(f"Error in full per-part pipeline: {str(e)}", exc_info=True)
            messagebox.showerror("Error", f"Full per-part pipeline error:\n{str(e)}")
        finally:
            try:
                self.root.after(
                    0,
                    lambda: start_button.configure(state="normal", text="Run Full Pipeline (Stage 2 → 3 → 4 per Part)")
                )
            except Exception:
                pass

    def is_json_format(self, text: str) -> bool:
        """
        Check if the response text is in JSON format
        
        Args:
            text: Response text to check
            
        Returns:
            True if text appears to be JSON format
        """
        if not text or not text.strip():
            return False
        
        # Try to parse as JSON
        try:
            # Remove markdown code blocks if present
            cleaned_text = text.strip()
            if cleaned_text.startswith('```'):
                # Extract content from code block
                lines = cleaned_text.split('\n')
                if len(lines) > 1:
                    # Skip first line (```json or ```)
                    cleaned_text = '\n'.join(lines[1:])
                    # Remove last line if it's ```
                    if cleaned_text.endswith('```'):
                        cleaned_text = cleaned_text[:-3].strip()
            
            # Try to parse as JSON
            json.loads(cleaned_text)
            return True
        except (json.JSONDecodeError, ValueError):
            # Check if it looks like JSON (starts with [ or {)
            cleaned_text = text.strip()
            if cleaned_text.startswith('[') or cleaned_text.startswith('{'):
                # Try to extract JSON from text
                try:
                    # Look for JSON array or object
                    if cleaned_text.startswith('['):
                        # Find matching closing bracket
                        bracket_count = 0
                        for i, char in enumerate(cleaned_text):
                            if char == '[':
                                bracket_count += 1
                            elif char == ']':
                                bracket_count -= 1
                                if bracket_count == 0:
                                    json_text = cleaned_text[:i+1]
                                    json.loads(json_text)
                                    return True
                    elif cleaned_text.startswith('{'):
                        # Find matching closing brace
                        brace_count = 0
                        for i, char in enumerate(cleaned_text):
                            if char == '{':
                                brace_count += 1
                            elif char == '}':
                                brace_count -= 1
                                if brace_count == 0:
                                    json_text = cleaned_text[:i+1]
                                    json.loads(json_text)
                                    return True
                except (json.JSONDecodeError, ValueError):
                    pass
        
        return False
    
    def parse_json_to_table_data(self, text: str) -> Optional[List[List[str]]]:
        """
        Parse JSON text into rows and columns for table display
        
        Args:
            text: JSON text to parse
            
        Returns:
            List of rows (each row is a list of cells) or None if parsing fails
        """
        try:
            # Remove markdown code blocks if present
            cleaned_text = text.strip()
            if cleaned_text.startswith('```'):
                lines = cleaned_text.split('\n')
                if len(lines) > 1:
                    cleaned_text = '\n'.join(lines[1:])
                    if cleaned_text.endswith('```'):
                        cleaned_text = cleaned_text[:-3].strip()
            
            # Parse JSON
            json_data = json.loads(cleaned_text)
            
            # Handle list of objects (most common case)
            if isinstance(json_data, list) and len(json_data) > 0:
                # Get all unique keys from all objects
                all_keys = set()
                for item in json_data:
                    if isinstance(item, dict):
                        all_keys.update(item.keys())
                
                # Sort keys for consistent column order
                headers = sorted(list(all_keys))
                
                # Create rows: first row is headers, then data rows
                rows = [headers]  # Header row
                
                for item in json_data:
                    if isinstance(item, dict):
                        row = [str(item.get(key, "")) for key in headers]
                        rows.append(row)
                    else:
                        # If item is not a dict, convert to string
                        rows.append([str(item)])
                
                return rows
            
            # Handle single object
            elif isinstance(json_data, dict):
                headers = sorted(list(json_data.keys()))
                rows = [headers]
                row = [str(json_data.get(key, "")) for key in headers]
                rows.append(row)
                return rows
            
            return None
        except (json.JSONDecodeError, ValueError, Exception):
            return None

    def show_response_window(self, response_text: str, file_path: Optional[str] = None, 
                            is_csv: bool = False, is_json: bool = False):
        """
        Show a window displaying the response text with options to save or copy.
        
        Args:
            response_text: The text to display
            file_path: Optional path to the file (for saving)
            is_csv: Whether the response is CSV format
            is_json: Whether the response is JSON format
        """
        window = ctk.CTkToplevel(self.root)
        window.title("Response Viewer")
        window.geometry("1000x700")
        window.minsize(800, 600)

        main_frame = ctk.CTkFrame(window)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)

        # Title
        title_text = "Response"
        if is_csv:
            title_text = "CSV Response"
        elif is_json:
            title_text = "JSON Response"
        
        title = ctk.CTkLabel(main_frame, text=title_text, 
                            font=ctk.CTkFont(size=20, weight="bold"))
        title.pack(pady=(0, 15))

        # Text display
        text_frame = ctk.CTkFrame(main_frame)
        text_frame.pack(fill="both", expand=True, pady=(0, 15))

        text_widget = ctk.CTkTextbox(text_frame, font=self.farsi_text_font, wrap="none")
        text_widget.pack(fill="both", expand=True, padx=10, pady=10)
        text_widget.insert("1.0", response_text)
        text_widget.configure(state="disabled")

        # Buttons frame
        buttons_frame = ctk.CTkFrame(main_frame)
        buttons_frame.pack(fill="x")

        def copy_to_clipboard():
            window.clipboard_clear()
            window.clipboard_append(response_text)
            messagebox.showinfo("Copied", "Response copied to clipboard!")

        def save_to_file():
            if file_path:
                try:
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(response_text)
                    messagebox.showinfo("Saved", f"Response saved to:\n{file_path}")
                except Exception as e:
                    messagebox.showerror("Error", f"Failed to save file:\n{str(e)}")
            else:
                filename = filedialog.asksaveasfilename(
                    defaultextension=".txt",
                    filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
                )
                if filename:
                    try:
                        with open(filename, "w", encoding="utf-8") as f:
                            f.write(response_text)
                        messagebox.showinfo("Saved", f"Response saved to:\n{filename}")
                    except Exception as e:
                        messagebox.showerror("Error", f"Failed to save file:\n{str(e)}")

        ctk.CTkButton(buttons_frame, text="Copy to Clipboard", 
                     command=copy_to_clipboard, width=150).pack(side="left", padx=5)
        ctk.CTkButton(buttons_frame, text="Save to File", 
                     command=save_to_file, width=150).pack(side="left", padx=5)
        ctk.CTkButton(buttons_frame, text="Close", 
                     command=window.destroy, width=100).pack(side="right", padx=5)

    def open_json_editor(self, json_path: Optional[str] = None):
        """Open JSON editor window"""
        if not json_path or not os.path.exists(json_path):
            messagebox.showerror("Error", "JSON file not found.")
            return

        try:
            with open(json_path, "r", encoding="utf-8") as f:
                json_content = f.read()
            
            self.show_response_window(json_content, json_path, is_json=True)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load JSON file:\n{str(e)}")

    def show_tabview(self):
        """Switch from main view to tabview"""
        self.main_stages_1_4_frame.pack_forget()
        self.main_tabview.pack(fill="both", expand=True, padx=10, pady=10)
        # Force switch to Stages 1-4 tab in tabview
        # Use after() to ensure the tabview is fully packed before setting tab
        def set_tab():
            try:
                self.main_tabview.set("Stages 1-4")
                # Double-check after a short delay
                self.root.after(50, lambda: self.main_tabview.set("Stages 1-4"))
            except Exception as e:
                self.logger.warning(f"Error setting tab to Stages 1-4: {e}")
        self.root.after(10, set_tab)
    
    def show_main_view(self):
        """Switch from tabview to main view"""
        self.main_tabview.pack_forget()
        self.main_stages_1_4_frame.pack(fill="both", expand=True, padx=10, pady=10)
    
    def setup_pipeline_status_bar(self):
        """Setup pipeline status indicator bar"""
        # Create status bar frame at the top
        status_bar_frame = ctk.CTkFrame(self.root)
        status_bar_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(status_bar_frame, text="Pipeline Status:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(side="left", padx=10)
        
        # Status indicators for each stage
        self.stage_status_labels = {}
        stages = ['E', 'F', 'J', 'H', 'V', 'M', 'L']
        
        for stage in stages:
            label = ctk.CTkLabel(status_bar_frame, text=f"{stage}: Waiting", 
                               text_color="gray", width=60)
            label.pack(side="left", padx=5)
            self.stage_status_labels[stage] = label
    
    def update_stage_status(self, stage: str, status: str, file_path: Optional[str] = None):
        """Update pipeline status for a stage"""
        if stage not in self.stage_status_labels:
            return
        
        label = self.stage_status_labels[stage]
        if status == "completed":
            label.configure(text=f"{stage}: OK", text_color="green")
        elif status == "processing":
            label.configure(text=f"{stage}: Processing", text_color="blue")
        elif status == "error":
            label.configure(text=f"{stage}: Error", text_color="red")
        else:
            label.configure(text=f"{stage}: Waiting", text_color="gray")
    
    def setup_stage_e_ui(self, parent):
        """Setup UI for Stage E: Image Notes Processing"""
        main_frame = ctk.CTkScrollableFrame(parent)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Navigation button to return to main view
        nav_frame = ctk.CTkFrame(main_frame)
        nav_frame.pack(fill="x", pady=(0, 10))
        ctk.CTkButton(
            nav_frame,
            text="← Back to Main View",
            command=self.show_main_view,
            width=150,
            height=30,
            font=ctk.CTkFont(size=12),
            fg_color="gray",
            hover_color="darkgray"
        ).pack(side="left", padx=10, pady=5)
        
        # Title
        title = ctk.CTkLabel(main_frame, text="Stage E: Image Notes Processing", 
                            font=ctk.CTkFont(size=24, weight="bold"))
        title.pack(pady=(0, 20))
        
        # Description
        desc = ctk.CTkLabel(
            main_frame, 
            text="Generate image notes from Stage 4 JSON and merge with Stage 4 data.",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        desc.pack(pady=(0, 20))
        
        # Stage 4 File Selection
        stage4_frame = ctk.CTkFrame(main_frame)
        stage4_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(stage4_frame, text="Stage 4 JSON (with PointId):", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        self.stage_e_stage4_var = ctk.StringVar()
        # Auto-fill if available
        if self.last_corrected_path and os.path.exists(self.last_corrected_path):
            self.stage_e_stage4_var.set(self.last_corrected_path)
        
        entry_frame = ctk.CTkFrame(stage4_frame)
        entry_frame.pack(fill="x", padx=10, pady=5)
        
        stage4_entry = ctk.CTkEntry(entry_frame, textvariable=self.stage_e_stage4_var)
        stage4_entry.pack(side="left", fill="x", expand=True, padx=5)
        
        ctk.CTkButton(entry_frame, text="Browse", 
                     command=lambda: self.browse_file_for_stage(self.stage_e_stage4_var, 
                                                                 filetypes=[("JSON", "*.json")])).pack(side="right")
        
        # Validation indicator
        self.stage_e_stage4_valid = ctk.CTkLabel(entry_frame, text="", width=30)
        self.stage_e_stage4_valid.pack(side="right", padx=5)
        
        # Stage 1 File Selection
        stage1_frame = ctk.CTkFrame(main_frame)
        stage1_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(stage1_frame, text="Stage 1 JSON:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        self.stage_e_stage1_var = ctk.StringVar()
        # Auto-fill if available
        if self.last_final_output_path and os.path.exists(self.last_final_output_path):
            self.stage_e_stage1_var.set(self.last_final_output_path)
        
        entry_frame1 = ctk.CTkFrame(stage1_frame)
        entry_frame1.pack(fill="x", padx=10, pady=5)
        
        stage1_entry = ctk.CTkEntry(entry_frame1, textvariable=self.stage_e_stage1_var)
        stage1_entry.pack(side="left", fill="x", expand=True, padx=5)
        
        ctk.CTkButton(entry_frame1, text="Browse", 
                     command=lambda: self.browse_file_for_stage(self.stage_e_stage1_var, 
                                                                 filetypes=[("JSON", "*.json")])).pack(side="right")
        
        # Validation indicator
        self.stage_e_stage1_valid = ctk.CTkLabel(entry_frame1, text="", width=30)
        self.stage_e_stage1_valid.pack(side="right", padx=5)
        
        # Prompt Section
        prompt_frame = ctk.CTkFrame(main_frame)
        prompt_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(prompt_frame, text="Prompt for Image Notes Generation:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        self.stage_e_prompt_text = ctk.CTkTextbox(prompt_frame, height=150, font=self.farsi_text_font)
        self.stage_e_prompt_text.pack(fill="x", padx=10, pady=5)
        
        # Model Selection
        model_frame = ctk.CTkFrame(main_frame)
        model_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(model_frame, text="Model:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        # Get default model from main model selection if available
        default_model = "gemini-2.5-flash"
        if hasattr(self, 'model_var') and self.model_var:
            default_model = self.model_var.get()
        
        self.stage_e_model_var = ctk.StringVar(value=default_model)
        self.stage_e_model_combo = ctk.CTkComboBox(
            model_frame,
            values=APIConfig.TEXT_MODELS,
            variable=self.stage_e_model_var,
            width=300
        )
        self.stage_e_model_combo.pack(anchor="w", padx=10, pady=5)
        
        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)
        
        self.stage_e_process_btn = ctk.CTkButton(
            process_btn_frame,
            text="Process Stage E",
            command=self.process_stage_e,
            width=200,
            height=40,
            font=ctk.CTkFont(size=16, weight="bold"),
            fg_color="blue"
        )
        self.stage_e_process_btn.pack(pady=10)
        
        # Status for Stage E
        self.stage_e_status_label = ctk.CTkLabel(main_frame, text="Ready", 
                                                 font=ctk.CTkFont(size=12), text_color="gray")
        self.stage_e_status_label.pack(pady=10)
        
        # Auto-validate files on change
        self.stage_e_stage4_var.trace('w', lambda *args: self.validate_stage_e_files())
        self.stage_e_stage1_var.trace('w', lambda *args: self.validate_stage_e_files())
    
    def browse_file_for_stage(self, var: ctk.StringVar, filetypes: list = None):
        """Browse for file and set variable"""
        if filetypes is None:
            filetypes = [("All files", "*.*")]
        
        filename = filedialog.askopenfilename(
            title="Select File",
            filetypes=filetypes
        )
        if filename:
            var.set(filename)
    
    def validate_stage_e_files(self):
        """Validate Stage E input files"""
        stage4_path = self.stage_e_stage4_var.get()
        stage1_path = self.stage_e_stage1_var.get()
        
        # Validate Stage 4
        if stage4_path and os.path.exists(stage4_path):
            try:
                data = json.load(open(stage4_path, 'r', encoding='utf-8'))
                points = data.get("data") or data.get("points") or data.get("rows", [])
                if points and points[0].get("PointId"):
                    self.stage_e_stage4_valid.configure(text="OK", text_color="green")
                else:
                    self.stage_e_stage4_valid.configure(text="W", text_color="orange")
            except:
                self.stage_e_stage4_valid.configure(text="X", text_color="red")
        else:
            self.stage_e_stage4_valid.configure(text="", text_color="gray")
        
        # Validate Stage 1
        if stage1_path and os.path.exists(stage1_path):
            try:
                data = json.load(open(stage1_path, 'r', encoding='utf-8'))
                rows = data.get("data") or data.get("rows", [])
                if rows:
                    self.stage_e_stage1_valid.configure(text="OK", text_color="green")
                else:
                    self.stage_e_stage1_valid.configure(text="W", text_color="orange")
            except:
                self.stage_e_stage1_valid.configure(text="X", text_color="red")
        else:
            self.stage_e_stage1_valid.configure(text="", text_color="gray")
    
    def process_stage_e(self):
        """Process Stage E in background thread"""
        def worker():
            try:
                self.stage_e_process_btn.configure(state="disabled", text="Processing...")
                self.update_stage_status("E", "processing")
                self.stage_e_status_label.configure(text="Processing Stage E...", text_color="blue")
                
                # Validate inputs
                stage4_path = self.stage_e_stage4_var.get().strip()
                stage1_path = self.stage_e_stage1_var.get().strip()
                prompt = self.stage_e_prompt_text.get("1.0", tk.END).strip()
                model_name = self.stage_e_model_var.get()
                
                if not stage4_path or not os.path.exists(stage4_path):
                    messagebox.showerror("Error", "Please select a valid Stage 4 JSON file")
                    return
                
                if not stage1_path or not os.path.exists(stage1_path):
                    messagebox.showerror("Error", "Please select a valid Stage 1 JSON file")
                    return
                
                if not prompt:
                    messagebox.showerror("Error", "Please enter a prompt for image notes generation")
                    return
                
                # Validate API keys
                if not self.api_key_manager.api_keys:
                    messagebox.showerror("Error", "Please load API keys first")
                    return
                
                def progress_callback(msg: str):
                    self.root.after(0, lambda: self.stage_e_status_label.configure(text=msg))
                
                # Process Stage E
                output_path = self.stage_e_processor.process_stage_e(
                    stage4_path=stage4_path,
                    stage1_path=stage1_path,
                    prompt=prompt,
                    model_name=model_name,
                    progress_callback=progress_callback
                )
                
                if output_path:
                    self.last_stage_e_path = output_path
                    self.update_stage_status("E", "completed", output_path)
                    self.stage_e_status_label.configure(
                        text=f"Stage E completed successfully!\nOutput: {os.path.basename(output_path)}",
                        text_color="green"
                    )
                    messagebox.showinfo("Success", f"Stage E completed!\n\nOutput saved to:\n{output_path}")
                else:
                    self.update_stage_status("E", "error")
                    self.stage_e_status_label.configure(text="Stage E failed. Check logs for details.", text_color="red")
                    messagebox.showerror("Error", "Stage E processing failed. Check logs for details.")
            
            except Exception as e:
                self.logger.error(f"Error in Stage E processing: {e}", exc_info=True)
                self.update_stage_status("E", "error")
                self.stage_e_status_label.configure(text=f"Error: {str(e)}", text_color="red")
                messagebox.showerror("Error", f"Stage E processing error:\n{str(e)}")
            finally:
                self.root.after(0, lambda: self.stage_e_process_btn.configure(state="normal", text="Process Stage E"))
        
        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def setup_stage_f_ui(self, parent):
        """Setup UI for Stage F: Image File Generation"""
        main_frame = ctk.CTkScrollableFrame(parent)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Navigation button to return to main view
        nav_frame = ctk.CTkFrame(main_frame)
        nav_frame.pack(fill="x", pady=(0, 10))
        ctk.CTkButton(
            nav_frame,
            text="← Back to Main View",
            command=self.show_main_view,
            width=150,
            height=30,
            font=ctk.CTkFont(size=12),
            fg_color="gray",
            hover_color="darkgray"
        ).pack(side="left", padx=10, pady=5)
        
        # Title
        title = ctk.CTkLabel(main_frame, text="Stage F: Image File Generation", 
                            font=ctk.CTkFont(size=24, weight="bold"))
        title.pack(pady=(0, 20))
        
        # Description
        desc = ctk.CTkLabel(
            main_frame, 
            text="Generate image file JSON from Stage E data.",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        desc.pack(pady=(0, 20))
        
        # Stage E File Selection
        stage_e_frame = ctk.CTkFrame(main_frame)
        stage_e_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(stage_e_frame, text="Stage E JSON:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        self.stage_f_stage_e_var = ctk.StringVar()
        # Auto-fill if available
        if hasattr(self, 'last_stage_e_path') and self.last_stage_e_path and os.path.exists(self.last_stage_e_path):
            self.stage_f_stage_e_var.set(self.last_stage_e_path)
        
        entry_frame = ctk.CTkFrame(stage_e_frame)
        entry_frame.pack(fill="x", padx=10, pady=5)
        
        stage_e_entry = ctk.CTkEntry(entry_frame, textvariable=self.stage_f_stage_e_var)
        stage_e_entry.pack(side="left", fill="x", expand=True, padx=5)
        
        ctk.CTkButton(entry_frame, text="Browse", 
                     command=lambda: self.browse_file_for_stage(self.stage_f_stage_e_var, 
                                                                 filetypes=[("JSON", "*.json")])).pack(side="right")
        
        # Validation indicator
        self.stage_f_stage_e_valid = ctk.CTkLabel(entry_frame, text="", width=30)
        self.stage_f_stage_e_valid.pack(side="right", padx=5)
        
        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)
        
        self.stage_f_process_btn = ctk.CTkButton(
            process_btn_frame,
            text="Process Stage F",
            command=self.process_stage_f,
            width=200,
            height=40,
            font=ctk.CTkFont(size=16, weight="bold"),
            fg_color="blue"
        )
        self.stage_f_process_btn.pack(pady=10)
        
        # Status for Stage F
        self.stage_f_status_label = ctk.CTkLabel(main_frame, text="Ready", 
                                                 font=ctk.CTkFont(size=12), text_color="gray")
        self.stage_f_status_label.pack(pady=10)
        
        # Auto-validate file on change
        self.stage_f_stage_e_var.trace('w', lambda *args: self.validate_stage_f_file())
    
    def validate_stage_f_file(self):
        """Validate Stage F input file"""
        stage_e_path = self.stage_f_stage_e_var.get()
        
        if stage_e_path and os.path.exists(stage_e_path):
            try:
                data = json.load(open(stage_e_path, 'r', encoding='utf-8'))
                metadata = data.get("metadata", {})
                if metadata.get("first_image_point_id"):
                    self.stage_f_stage_e_valid.configure(text="OK", text_color="green")
                else:
                    self.stage_f_stage_e_valid.configure(text="W", text_color="orange")
            except:
                self.stage_f_stage_e_valid.configure(text="X", text_color="red")
        else:
            self.stage_f_stage_e_valid.configure(text="", text_color="gray")
    
    def process_stage_f(self):
        """Process Stage F in background thread"""
        def worker():
            try:
                self.stage_f_process_btn.configure(state="disabled", text="Processing...")
                self.update_stage_status("F", "processing")
                self.stage_f_status_label.configure(text="Processing Stage F...", text_color="blue")
                
                # Validate inputs
                stage_e_path = self.stage_f_stage_e_var.get().strip()
                
                if not stage_e_path or not os.path.exists(stage_e_path):
                    messagebox.showerror("Error", "Please select a valid Stage E JSON file")
                    return
                
                def progress_callback(msg: str):
                    self.root.after(0, lambda: self.stage_f_status_label.configure(text=msg))
                
                # Process Stage F
                output_path = self.stage_f_processor.process_stage_f(
                    stage_e_path=stage_e_path,
                    progress_callback=progress_callback
                )
                
                if output_path:
                    self.last_stage_f_path = output_path
                    self.update_stage_status("F", "completed", output_path)
                    self.stage_f_status_label.configure(
                        text=f"Stage F completed successfully!\nOutput: {os.path.basename(output_path)}",
                        text_color="green"
                    )
                    messagebox.showinfo("Success", f"Stage F completed!\n\nOutput saved to:\n{output_path}")
                else:
                    self.update_stage_status("F", "error")
                    self.stage_f_status_label.configure(text="Stage F failed. Check logs for details.", text_color="red")
                    messagebox.showerror("Error", "Stage F processing failed. Check logs for details.")
            
            except Exception as e:
                self.logger.error(f"Error in Stage F processing: {e}", exc_info=True)
                self.update_stage_status("F", "error")
                self.stage_f_status_label.configure(text=f"Error: {str(e)}", text_color="red")
                messagebox.showerror("Error", f"Stage F processing error:\n{str(e)}")
            finally:
                self.root.after(0, lambda: self.stage_f_process_btn.configure(state="normal", text="Process Stage F"))
        
        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def setup_stage_j_ui(self, parent):
        """Setup UI for Stage J: Add Imp & Type"""
        main_frame = ctk.CTkScrollableFrame(parent)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Navigation button to return to main view
        nav_frame = ctk.CTkFrame(main_frame)
        nav_frame.pack(fill="x", pady=(0, 10))
        ctk.CTkButton(
            nav_frame,
            text="< Back to Main View",
            command=self.show_main_view,
            width=150,
            height=30,
            font=ctk.CTkFont(size=12),
            fg_color="gray",
            hover_color="darkgray"
        ).pack(side="left", padx=10, pady=5)
        
        # Title
        title = ctk.CTkLabel(main_frame, text="Stage J: Add Imp & Type", 
                            font=ctk.CTkFont(size=24, weight="bold"))
        title.pack(pady=(0, 20))
        
        # Description
        desc = ctk.CTkLabel(
            main_frame, 
            text="Add Imp and Type columns to Stage E data based on Word test file.",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        desc.pack(pady=(0, 20))
        
        # Stage E File Selection
        stage_e_frame = ctk.CTkFrame(main_frame)
        stage_e_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(stage_e_frame, text="Stage E JSON:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_j_stage_e_var'):
            self.stage_j_stage_e_var = ctk.StringVar()
        # Auto-fill if available
        if hasattr(self, 'last_stage_e_path') and self.last_stage_e_path and os.path.exists(self.last_stage_e_path):
            self.stage_j_stage_e_var.set(self.last_stage_e_path)
        
        entry_frame = ctk.CTkFrame(stage_e_frame)
        entry_frame.pack(fill="x", padx=10, pady=5)
        
        stage_e_entry = ctk.CTkEntry(entry_frame, textvariable=self.stage_j_stage_e_var)
        stage_e_entry.pack(side="left", fill="x", expand=True, padx=5)
        
        ctk.CTkButton(entry_frame, text="Browse", 
                     command=lambda: self.browse_file_for_stage(self.stage_j_stage_e_var, 
                                                                 filetypes=[("JSON", "*.json")])).pack(side="right")
        
        # Validation indicator
        if not hasattr(self, 'stage_j_stage_e_valid'):
            self.stage_j_stage_e_valid = ctk.CTkLabel(entry_frame, text="", width=30)
        self.stage_j_stage_e_valid.pack(side="right", padx=5)
        
        # Word File Selection
        word_frame = ctk.CTkFrame(main_frame)
        word_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(word_frame, text="Word File (Test Questions):", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_j_word_var'):
            self.stage_j_word_var = ctk.StringVar()
        
        entry_frame_word = ctk.CTkFrame(word_frame)
        entry_frame_word.pack(fill="x", padx=10, pady=5)
        
        word_entry = ctk.CTkEntry(entry_frame_word, textvariable=self.stage_j_word_var)
        word_entry.pack(side="left", fill="x", expand=True, padx=5)
        
        ctk.CTkButton(entry_frame_word, text="Browse", 
                     command=lambda: self.browse_file_for_stage(self.stage_j_word_var, 
                                                                 filetypes=[("Word Documents", "*.docx *.doc"), ("All files", "*.*")])).pack(side="right")
        
        # Validation indicator for Word file
        if not hasattr(self, 'stage_j_word_valid'):
            self.stage_j_word_valid = ctk.CTkLabel(entry_frame_word, text="", width=30)
        self.stage_j_word_valid.pack(side="right", padx=5)
        
        # Stage F File Selection (Optional)
        stage_f_frame = ctk.CTkFrame(main_frame)
        stage_f_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(stage_f_frame, text="Stage F JSON (Optional - f.json):", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_j_stage_f_var'):
            self.stage_j_stage_f_var = ctk.StringVar()
        # Auto-fill if available (try to find f.json in same directory as Stage E)
        if hasattr(self, 'last_stage_f_path') and self.last_stage_f_path and os.path.exists(self.last_stage_f_path):
            self.stage_j_stage_f_var.set(self.last_stage_f_path)
        elif self.stage_j_stage_e_var.get():
            # Try to find f.json in same directory
            stage_e_dir = os.path.dirname(self.stage_j_stage_e_var.get())
            f_json_path = os.path.join(stage_e_dir, "f.json")
            if os.path.exists(f_json_path):
                self.stage_j_stage_f_var.set(f_json_path)
        
        entry_frame_f = ctk.CTkFrame(stage_f_frame)
        entry_frame_f.pack(fill="x", padx=10, pady=5)
        
        stage_f_entry = ctk.CTkEntry(entry_frame_f, textvariable=self.stage_j_stage_f_var)
        stage_f_entry.pack(side="left", fill="x", expand=True, padx=5)
        
        ctk.CTkButton(entry_frame_f, text="Browse", 
                     command=lambda: self.browse_file_for_stage(self.stage_j_stage_f_var, 
                                                                 filetypes=[("JSON", "*.json")])).pack(side="right")
        
        # Validation indicator for Stage F file
        if not hasattr(self, 'stage_j_stage_f_valid'):
            self.stage_j_stage_f_valid = ctk.CTkLabel(entry_frame_f, text="", width=30)
        self.stage_j_stage_f_valid.pack(side="right", padx=5)
        
        # Prompt Section
        prompt_frame = ctk.CTkFrame(main_frame)
        prompt_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(prompt_frame, text="Prompt for Imp & Type Generation:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_j_prompt_text'):
            self.stage_j_prompt_text = ctk.CTkTextbox(prompt_frame, height=150, font=self.farsi_text_font)
        self.stage_j_prompt_text.pack(fill="x", padx=10, pady=5)
        
        # Model Selection
        model_frame = ctk.CTkFrame(main_frame)
        model_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(model_frame, text="Model:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        # Get default model from main model selection if available
        default_model = "gemini-2.5-flash"
        if hasattr(self, 'model_var') and self.model_var:
            default_model = self.model_var.get()
        
        if not hasattr(self, 'stage_j_model_var'):
            self.stage_j_model_var = ctk.StringVar(value=default_model)
        if not hasattr(self, 'stage_j_model_combo'):
            self.stage_j_model_combo = ctk.CTkComboBox(
                model_frame,
                values=APIConfig.TEXT_MODELS,
                variable=self.stage_j_model_var,
                width=300
            )
        self.stage_j_model_combo.pack(anchor="w", padx=10, pady=5)
        
        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)
        
        if not hasattr(self, 'stage_j_process_btn'):
            self.stage_j_process_btn = ctk.CTkButton(
                process_btn_frame,
                text="Process Stage J",
                command=self.process_stage_j,
                width=200,
                height=40,
                font=ctk.CTkFont(size=16, weight="bold"),
                fg_color="blue"
            )
        self.stage_j_process_btn.pack(pady=10)
        
        # Status for Stage J
        if not hasattr(self, 'stage_j_status_label'):
            self.stage_j_status_label = ctk.CTkLabel(main_frame, text="Ready", 
                                                     font=ctk.CTkFont(size=12), text_color="gray")
        self.stage_j_status_label.pack(pady=10)
        
        # Auto-validate files on change
        self.stage_j_stage_e_var.trace('w', lambda *args: self.validate_stage_j_files())
        self.stage_j_word_var.trace('w', lambda *args: self.validate_stage_j_files())
        self.stage_j_stage_f_var.trace('w', lambda *args: self.validate_stage_j_files())
    
    def validate_stage_j_files(self):
        """Validate Stage J input files"""
        stage_e_path = self.stage_j_stage_e_var.get()
        word_path = self.stage_j_word_var.get()
        stage_f_path = self.stage_j_stage_f_var.get() if hasattr(self, 'stage_j_stage_f_var') else ""
        
        # Validate Stage E
        if stage_e_path and os.path.exists(stage_e_path):
            try:
                data = json.load(open(stage_e_path, 'r', encoding='utf-8'))
                records = data.get("data") or data.get("rows", [])
                if records and records[0].get("PointId"):
                    self.stage_j_stage_e_valid.configure(text="OK", text_color="green")
                else:
                    self.stage_j_stage_e_valid.configure(text="W", text_color="orange")
            except:
                self.stage_j_stage_e_valid.configure(text="X", text_color="red")
        else:
            self.stage_j_stage_e_valid.configure(text="", text_color="gray")
        
        # Validate Word file
        if word_path and os.path.exists(word_path):
            # Check if it's a Word file
            ext = os.path.splitext(word_path)[1].lower()
            if ext in ['.docx', '.doc']:
                self.stage_j_word_valid.configure(text="OK", text_color="green")
            else:
                self.stage_j_word_valid.configure(text="W", text_color="orange")
        else:
            self.stage_j_word_valid.configure(text="", text_color="gray")
        
        # Validate Stage F (optional)
        if hasattr(self, 'stage_j_stage_f_valid'):
            if stage_f_path and os.path.exists(stage_f_path):
                try:
                    data = json.load(open(stage_f_path, 'r', encoding='utf-8'))
                    records = data.get("data") or data.get("rows", [])
                    if records:
                        self.stage_j_stage_f_valid.configure(text="OK", text_color="green")
                    else:
                        self.stage_j_stage_f_valid.configure(text="W", text_color="orange")
                except:
                    self.stage_j_stage_f_valid.configure(text="X", text_color="red")
            else:
                self.stage_j_stage_f_valid.configure(text="", text_color="gray")
    
    def process_stage_j(self):
        """Process Stage J in background thread"""
        def worker():
            try:
                self.stage_j_process_btn.configure(state="disabled", text="Processing...")
                self.update_stage_status("J", "processing")
                self.stage_j_status_label.configure(text="Processing Stage J...", text_color="blue")
                
                # Validate inputs
                stage_e_path = self.stage_j_stage_e_var.get().strip()
                word_path = self.stage_j_word_var.get().strip()
                stage_f_path = self.stage_j_stage_f_var.get().strip() if hasattr(self, 'stage_j_stage_f_var') else ""
                prompt = self.stage_j_prompt_text.get("1.0", tk.END).strip()
                model_name = self.stage_j_model_var.get()
                
                if not stage_e_path or not os.path.exists(stage_e_path):
                    messagebox.showerror("Error", "Please select a valid Stage E JSON file")
                    return
                
                if not word_path or not os.path.exists(word_path):
                    messagebox.showerror("Error", "Please select a valid Word file")
                    return
                
                if not prompt:
                    messagebox.showerror("Error", "Please enter a prompt for Imp & Type generation")
                    return
                
                # Stage F is optional, but validate if provided
                if stage_f_path and not os.path.exists(stage_f_path):
                    messagebox.showerror("Error", "Stage F JSON file path is invalid")
                    return
                
                # Validate API keys
                if not self.api_key_manager.api_keys:
                    messagebox.showerror("Error", "Please load API keys first")
                    return
                
                def progress_callback(msg: str):
                    self.root.after(0, lambda: self.stage_j_status_label.configure(text=msg))
                
                # Process Stage J
                output_path = self.stage_j_processor.process_stage_j(
                    stage_e_path=stage_e_path,
                    word_file_path=word_path,
                    stage_f_path=stage_f_path if stage_f_path else None,
                    prompt=prompt,
                    model_name=model_name,
                    progress_callback=progress_callback
                )
                
                if output_path:
                    self.last_stage_j_path = output_path
                    self.update_stage_status("J", "completed", output_path)
                    self.stage_j_status_label.configure(
                        text=f"Stage J completed successfully!\nOutput: {os.path.basename(output_path)}",
                        text_color="green"
                    )
                    messagebox.showinfo("Success", f"Stage J completed!\n\nOutput saved to:\n{output_path}")
                else:
                    self.update_stage_status("J", "error")
                    self.stage_j_status_label.configure(text="Stage J failed. Check logs for details.", text_color="red")
                    messagebox.showerror("Error", "Stage J processing failed. Check logs for details.")
            
            except Exception as e:
                self.logger.error(f"Error in Stage J processing: {e}", exc_info=True)
                self.update_stage_status("J", "error")
                self.stage_j_status_label.configure(text=f"Error: {str(e)}", text_color="red")
                messagebox.showerror("Error", f"Stage J processing error:\n{str(e)}")
            finally:
                self.root.after(0, lambda: self.stage_j_process_btn.configure(state="normal", text="Process Stage J"))
        
        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def validate_stage_h_files(self):
        """Validate Stage H input files"""
        stage_j_path = self.stage_h_stage_j_var.get()
        stage_f_path = self.stage_h_stage_f_var.get()
        
        # Validate Stage J
        if stage_j_path and os.path.exists(stage_j_path):
            try:
                data = json.load(open(stage_j_path, 'r', encoding='utf-8'))
                records = data.get("data") or data.get("rows", [])
                if records and records[0].get("PointId"):
                    self.stage_h_stage_j_valid.configure(text="OK", text_color="green")
                else:
                    self.stage_h_stage_j_valid.configure(text="W", text_color="orange")
            except:
                self.stage_h_stage_j_valid.configure(text="X", text_color="red")
        else:
            self.stage_h_stage_j_valid.configure(text="", text_color="gray")
        
        # Validate Stage F
        if stage_f_path and os.path.exists(stage_f_path):
            try:
                data = json.load(open(stage_f_path, 'r', encoding='utf-8'))
                records = data.get("data") or data.get("rows", [])
                if records:
                    self.stage_h_stage_f_valid.configure(text="OK", text_color="green")
                else:
                    self.stage_h_stage_f_valid.configure(text="W", text_color="orange")
            except:
                self.stage_h_stage_f_valid.configure(text="X", text_color="red")
        else:
            self.stage_h_stage_f_valid.configure(text="", text_color="gray")
    
    def process_stage_h(self):
        """Process Stage H in background thread"""
        def worker():
            try:
                self.stage_h_process_btn.configure(state="disabled", text="Processing...")
                self.update_stage_status("H", "processing")
                self.stage_h_status_label.configure(text="Processing Stage H...", text_color="blue")
                
                # Validate inputs
                stage_j_path = self.stage_h_stage_j_var.get().strip()
                stage_f_path = self.stage_h_stage_f_var.get().strip()
                prompt = self.stage_h_prompt_text.get("1.0", tk.END).strip()
                model_name = self.stage_h_model_var.get()
                
                if not stage_j_path or not os.path.exists(stage_j_path):
                    messagebox.showerror("Error", "Please select a valid Stage J JSON file")
                    return
                
                if not stage_f_path or not os.path.exists(stage_f_path):
                    messagebox.showerror("Error", "Please select a valid Stage F JSON file")
                    return
                
                if not prompt:
                    messagebox.showerror("Error", "Please enter a prompt for flashcard generation")
                    return
                
                # Validate API keys
                if not self.api_key_manager.api_keys:
                    messagebox.showerror("Error", "Please load API keys first")
                    return
                
                def progress_callback(msg: str):
                    self.root.after(0, lambda: self.stage_h_status_label.configure(text=msg))
                
                # Process Stage H
                output_path = self.stage_h_processor.process_stage_h(
                    stage_j_path=stage_j_path,
                    stage_f_path=stage_f_path,
                    prompt=prompt,
                    model_name=model_name,
                    progress_callback=progress_callback
                )
                
                if output_path:
                    self.update_stage_status("H", "completed", output_path)
                    self.stage_h_status_label.configure(
                        text=f"Stage H completed successfully!\nOutput: {os.path.basename(output_path)}",
                        text_color="green"
                    )
                    messagebox.showinfo("Success", f"Stage H completed!\n\nOutput saved to:\n{output_path}")
                else:
                    self.update_stage_status("H", "error")
                    self.stage_h_status_label.configure(text="Stage H failed. Check logs for details.", text_color="red")
                    messagebox.showerror("Error", "Stage H processing failed. Check logs for details.")
            
            except Exception as e:
                self.logger.error(f"Error in Stage H processing: {e}", exc_info=True)
                self.update_stage_status("H", "error")
                self.stage_h_status_label.configure(text=f"Error: {str(e)}", text_color="red")
                messagebox.showerror("Error", f"Stage H processing error:\n{str(e)}")
            finally:
                self.root.after(0, lambda: self.stage_h_process_btn.configure(state="normal", text="Process Stage H"))
        
        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def setup_stage_h_ui(self, parent):
        """Setup UI for Stage H: Flashcard Generation"""
        main_frame = ctk.CTkScrollableFrame(parent)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        title = ctk.CTkLabel(main_frame, text="Stage H: Flashcard Generation", 
                            font=ctk.CTkFont(size=24, weight="bold"))
        title.pack(pady=(0, 20))
        
        desc = ctk.CTkLabel(main_frame, text="Generate flashcards from Stage J and Stage F data.", 
                           font=ctk.CTkFont(size=12), text_color="gray")
        desc.pack(pady=(0, 20))
        
        # Stage J File Selection
        stage_j_frame = ctk.CTkFrame(main_frame)
        stage_j_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(stage_j_frame, text="Stage J JSON (a file):", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_h_stage_j_var'):
            self.stage_h_stage_j_var = ctk.StringVar()
        # Auto-fill if available
        if hasattr(self, 'last_stage_j_path') and self.last_stage_j_path and os.path.exists(self.last_stage_j_path):
            self.stage_h_stage_j_var.set(self.last_stage_j_path)
        
        entry_frame = ctk.CTkFrame(stage_j_frame)
        entry_frame.pack(fill="x", padx=10, pady=5)
        
        stage_j_entry = ctk.CTkEntry(entry_frame, textvariable=self.stage_h_stage_j_var)
        stage_j_entry.pack(side="left", fill="x", expand=True, padx=5)
        
        ctk.CTkButton(entry_frame, text="Browse", 
                     command=lambda: self.browse_file_for_stage(self.stage_h_stage_j_var, 
                                                                 filetypes=[("JSON", "*.json")])).pack(side="right")
        
        # Validation indicator
        if not hasattr(self, 'stage_h_stage_j_valid'):
            self.stage_h_stage_j_valid = ctk.CTkLabel(entry_frame, text="", width=30)
        self.stage_h_stage_j_valid.pack(side="right", padx=5)
        
        # Stage F File Selection
        stage_f_frame = ctk.CTkFrame(main_frame)
        stage_f_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(stage_f_frame, text="Stage F JSON (f.json):", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_h_stage_f_var'):
            self.stage_h_stage_f_var = ctk.StringVar()
        # Auto-fill if available (try to find f.json in same directory as Stage J)
        if hasattr(self, 'last_stage_f_path') and self.last_stage_f_path and os.path.exists(self.last_stage_f_path):
            self.stage_h_stage_f_var.set(self.last_stage_f_path)
        elif self.stage_h_stage_j_var.get():
            # Try to find f.json in same directory
            stage_j_dir = os.path.dirname(self.stage_h_stage_j_var.get())
            f_json_path = os.path.join(stage_j_dir, "f.json")
            if os.path.exists(f_json_path):
                self.stage_h_stage_f_var.set(f_json_path)
        
        entry_frame_f = ctk.CTkFrame(stage_f_frame)
        entry_frame_f.pack(fill="x", padx=10, pady=5)
        
        stage_f_entry = ctk.CTkEntry(entry_frame_f, textvariable=self.stage_h_stage_f_var)
        stage_f_entry.pack(side="left", fill="x", expand=True, padx=5)
        
        ctk.CTkButton(entry_frame_f, text="Browse", 
                     command=lambda: self.browse_file_for_stage(self.stage_h_stage_f_var, 
                                                                 filetypes=[("JSON", "*.json")])).pack(side="right")
        
        # Validation indicator for Stage F file
        if not hasattr(self, 'stage_h_stage_f_valid'):
            self.stage_h_stage_f_valid = ctk.CTkLabel(entry_frame_f, text="", width=30)
        self.stage_h_stage_f_valid.pack(side="right", padx=5)
        
        # Prompt Section
        prompt_frame = ctk.CTkFrame(main_frame)
        prompt_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(prompt_frame, text="Prompt for Flashcard Generation:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_h_prompt_text'):
            self.stage_h_prompt_text = ctk.CTkTextbox(prompt_frame, height=150, font=self.farsi_text_font)
        self.stage_h_prompt_text.pack(fill="x", padx=10, pady=5)
        
        # Model Selection
        model_frame = ctk.CTkFrame(main_frame)
        model_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(model_frame, text="Model:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        # Get default model from main model selection if available
        default_model = "gemini-2.5-flash"
        if hasattr(self, 'model_var') and self.model_var:
            default_model = self.model_var.get()
        
        if not hasattr(self, 'stage_h_model_var'):
            self.stage_h_model_var = ctk.StringVar(value=default_model)
        if not hasattr(self, 'stage_h_model_combo'):
            self.stage_h_model_combo = ctk.CTkComboBox(
                model_frame,
                values=APIConfig.TEXT_MODELS,
                variable=self.stage_h_model_var,
                width=300
            )
        self.stage_h_model_combo.pack(anchor="w", padx=10, pady=5)
        
        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)
        
        if not hasattr(self, 'stage_h_process_btn'):
            self.stage_h_process_btn = ctk.CTkButton(
                process_btn_frame,
                text="Process Stage H",
                command=self.process_stage_h,
                font=ctk.CTkFont(size=16, weight="bold"),
                height=40
            )
        self.stage_h_process_btn.pack(pady=10)
        
        # Status Label
        if not hasattr(self, 'stage_h_status_label'):
            self.stage_h_status_label = ctk.CTkLabel(
                process_btn_frame,
                text="Ready",
                font=ctk.CTkFont(size=12),
                text_color="gray"
            )
        self.stage_h_status_label.pack(pady=5)
        
        # Bind validation
        self.stage_h_stage_j_var.trace('w', lambda *args: self.validate_stage_h_files())
        self.stage_h_stage_f_var.trace('w', lambda *args: self.validate_stage_h_files())
        
        # Initial validation
        self.validate_stage_h_files()
    
    def setup_stage_v_ui(self, parent):
        """Setup UI for Stage V: Test File Generation"""
        main_frame = ctk.CTkScrollableFrame(parent)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        title = ctk.CTkLabel(main_frame, text="Stage V: Test File Generation", 
                            font=ctk.CTkFont(size=24, weight="bold"))
        title.pack(pady=(0, 20))
        
        desc = ctk.CTkLabel(main_frame, text="Generate test files from Stage J data and Word document in two steps.", 
                           font=ctk.CTkFont(size=12), text_color="gray")
        desc.pack(pady=(0, 20))
        
        # Stage J File Selection
        stage_j_frame = ctk.CTkFrame(main_frame)
        stage_j_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(stage_j_frame, text="Stage J JSON (a file):", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_v_stage_j_var'):
            self.stage_v_stage_j_var = ctk.StringVar()
        # Auto-fill if available
        if hasattr(self, 'last_stage_j_path') and self.last_stage_j_path and os.path.exists(self.last_stage_j_path):
            self.stage_v_stage_j_var.set(self.last_stage_j_path)
        
        entry_frame = ctk.CTkFrame(stage_j_frame)
        entry_frame.pack(fill="x", padx=10, pady=5)
        
        stage_j_entry = ctk.CTkEntry(entry_frame, textvariable=self.stage_v_stage_j_var)
        stage_j_entry.pack(side="left", fill="x", expand=True, padx=5)
        
        ctk.CTkButton(entry_frame, text="Browse", 
                     command=lambda: self.browse_file_for_stage(self.stage_v_stage_j_var, 
                                                                 filetypes=[("JSON", "*.json")])).pack(side="right")
        
        # Validation indicator
        if not hasattr(self, 'stage_v_stage_j_valid'):
            self.stage_v_stage_j_valid = ctk.CTkLabel(entry_frame, text="", width=30)
        self.stage_v_stage_j_valid.pack(side="right", padx=5)
        
        # Word File Selection
        word_frame = ctk.CTkFrame(main_frame)
        word_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(word_frame, text="Word Document (Test Questions):", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_v_word_var'):
            self.stage_v_word_var = ctk.StringVar()
        
        entry_frame_word = ctk.CTkFrame(word_frame)
        entry_frame_word.pack(fill="x", padx=10, pady=5)
        
        word_entry = ctk.CTkEntry(entry_frame_word, textvariable=self.stage_v_word_var)
        word_entry.pack(side="left", fill="x", expand=True, padx=5)
        
        ctk.CTkButton(entry_frame_word, text="Browse", 
                     command=lambda: self.browse_file_for_stage(self.stage_v_word_var, 
                                                                 filetypes=[("Word Documents", "*.docx *.doc")])).pack(side="right")
        
        # Validation indicator for Word file
        if not hasattr(self, 'stage_v_word_valid'):
            self.stage_v_word_valid = ctk.CTkLabel(entry_frame_word, text="", width=30)
        self.stage_v_word_valid.pack(side="right", padx=5)
        
        # Step 1 Section
        step1_frame = ctk.CTkFrame(main_frame)
        step1_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(step1_frame, text="Step 1: Generate Initial Test Questions", 
                    font=ctk.CTkFont(size=16, weight="bold")).pack(anchor="w", padx=10, pady=10)
        
        # Step 1 Prompt
        ctk.CTkLabel(step1_frame, text="Prompt for Step 1:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_v_prompt1_text'):
            self.stage_v_prompt1_text = ctk.CTkTextbox(step1_frame, height=150, font=self.farsi_text_font)
        self.stage_v_prompt1_text.pack(fill="x", padx=10, pady=5)
        
        # Step 1 Model Selection
        model1_frame = ctk.CTkFrame(step1_frame)
        model1_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(model1_frame, text="Model for Step 1:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(side="left", padx=5)
        
        default_model = "gemini-2.5-flash"
        if hasattr(self, 'model_var') and self.model_var:
            default_model = self.model_var.get()
        
        if not hasattr(self, 'stage_v_model1_var'):
            self.stage_v_model1_var = ctk.StringVar(value=default_model)
        if not hasattr(self, 'stage_v_model1_combo'):
            self.stage_v_model1_combo = ctk.CTkComboBox(
                model1_frame,
                values=APIConfig.TEXT_MODELS,
                variable=self.stage_v_model1_var,
                width=300
            )
        self.stage_v_model1_combo.pack(side="left", padx=5)
        
        # Step 2 Section
        step2_frame = ctk.CTkFrame(main_frame)
        step2_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(step2_frame, text="Step 2: Refine and Add QId Mapping", 
                    font=ctk.CTkFont(size=16, weight="bold")).pack(anchor="w", padx=10, pady=10)
        
        # Step 2 Prompt
        ctk.CTkLabel(step2_frame, text="Prompt for Step 2:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_v_prompt2_text'):
            self.stage_v_prompt2_text = ctk.CTkTextbox(step2_frame, height=150, font=self.farsi_text_font)
        self.stage_v_prompt2_text.pack(fill="x", padx=10, pady=5)
        
        # Step 2 Model Selection
        model2_frame = ctk.CTkFrame(step2_frame)
        model2_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(model2_frame, text="Model for Step 2:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(side="left", padx=5)
        
        if not hasattr(self, 'stage_v_model2_var'):
            self.stage_v_model2_var = ctk.StringVar(value=default_model)
        if not hasattr(self, 'stage_v_model2_combo'):
            self.stage_v_model2_combo = ctk.CTkComboBox(
                model2_frame,
                values=APIConfig.TEXT_MODELS,
                variable=self.stage_v_model2_var,
                width=300
            )
        self.stage_v_model2_combo.pack(side="left", padx=5)
        
        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)
        
        if not hasattr(self, 'stage_v_process_btn'):
            self.stage_v_process_btn = ctk.CTkButton(
                process_btn_frame,
                text="Process Stage V",
                command=self.process_stage_v,
                font=ctk.CTkFont(size=16, weight="bold"),
                height=40
            )
        self.stage_v_process_btn.pack(pady=10)
        
        # Status Label
        if not hasattr(self, 'stage_v_status_label'):
            self.stage_v_status_label = ctk.CTkLabel(
                process_btn_frame,
                text="Ready",
                font=ctk.CTkFont(size=12),
                text_color="gray"
            )
        self.stage_v_status_label.pack(pady=5)
        
        # Bind validation
        self.stage_v_stage_j_var.trace('w', lambda *args: self.validate_stage_v_files())
        self.stage_v_word_var.trace('w', lambda *args: self.validate_stage_v_files())
        
        # Initial validation
        self.validate_stage_v_files()
    
    def validate_stage_v_files(self):
        """Validate Stage V input files"""
        stage_j_path = self.stage_v_stage_j_var.get().strip()
        word_path = self.stage_v_word_var.get().strip()
        
        # Validate Stage J file
        if stage_j_path and os.path.exists(stage_j_path):
            self.stage_v_stage_j_valid.configure(text="Ok", text_color="green")
        else:
            self.stage_v_stage_j_valid.configure(text="", text_color="red")
        
        # Validate Word file
        if word_path and os.path.exists(word_path):
            self.stage_v_word_valid.configure(text="Ok", text_color="green")
        else:
            self.stage_v_word_valid.configure(text="", text_color="red")
    
    def process_stage_v(self):
        """Process Stage V in background thread"""
        def worker():
            try:
                self.stage_v_process_btn.configure(state="disabled", text="Processing...")
                self.update_stage_status("V", "processing")
                self.stage_v_status_label.configure(text="Processing Stage V...", text_color="blue")
                
                # Validate inputs
                stage_j_path = self.stage_v_stage_j_var.get().strip()
                word_path = self.stage_v_word_var.get().strip()
                prompt_1 = self.stage_v_prompt1_text.get("1.0", tk.END).strip()
                model_name_1 = self.stage_v_model1_var.get()
                prompt_2 = self.stage_v_prompt2_text.get("1.0", tk.END).strip()
                model_name_2 = self.stage_v_model2_var.get()
                
                if not stage_j_path or not os.path.exists(stage_j_path):
                    messagebox.showerror("Error", "Please select a valid Stage J JSON file")
                    return
                
                if not word_path or not os.path.exists(word_path):
                    messagebox.showerror("Error", "Please select a valid Word document")
                    return
                
                if not prompt_1:
                    messagebox.showerror("Error", "Please enter a prompt for Step 1")
                    return
                
                if not prompt_2:
                    messagebox.showerror("Error", "Please enter a prompt for Step 2")
                    return
                
                # Validate API keys
                if not self.api_key_manager.api_keys:
                    messagebox.showerror("Error", "Please load API keys first")
                    return
                
                def progress_callback(msg: str):
                    self.root.after(0, lambda: self.stage_v_status_label.configure(text=msg))
                
                # Process Stage V
                output_path = self.stage_v_processor.process_stage_v(
                    stage_j_path=stage_j_path,
                    word_file_path=word_path,
                    prompt_1=prompt_1,
                    model_name_1=model_name_1,
                    prompt_2=prompt_2,
                    model_name_2=model_name_2,
                    progress_callback=progress_callback
                )
                
                if output_path:
                    self.update_stage_status("V", "completed", output_path)
                    self.stage_v_status_label.configure(
                        text=f"Stage V completed successfully!\nOutput: {os.path.basename(output_path)}",
                        text_color="green"
                    )
                    messagebox.showinfo("Success", f"Stage V completed!\n\nOutput saved to:\n{output_path}")
                else:
                    self.update_stage_status("V", "error")
                    self.stage_v_status_label.configure(text="Stage V failed. Check logs for details.", text_color="red")
                    messagebox.showerror("Error", "Stage V processing failed. Check logs for details.")
            
            except Exception as e:
                self.logger.error(f"Error in Stage V processing: {e}", exc_info=True)
                self.update_stage_status("V", "error")
                self.stage_v_status_label.configure(text=f"Error: {str(e)}", text_color="red")
                messagebox.showerror("Error", f"Stage V processing error:\n{str(e)}")
            finally:
                self.root.after(0, lambda: self.stage_v_process_btn.configure(state="normal", text="Process Stage V"))
        
        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def setup_stage_m_ui(self, parent):
        """Setup UI for Stage M: Topic ID List"""
        main_frame = ctk.CTkScrollableFrame(parent)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        title = ctk.CTkLabel(main_frame, text="Stage M: Topic ID List", 
                            font=ctk.CTkFont(size=24, weight="bold"))
        title.pack(pady=(0, 20))
        
        desc = ctk.CTkLabel(main_frame, text="Extract unique chapter / subchapter / topic combinations from Stage H (ac) file.", 
                           font=ctk.CTkFont(size=12), text_color="gray")
        desc.pack(pady=(0, 20))

        # Stage H File Selection
        stage_h_frame = ctk.CTkFrame(main_frame)
        stage_h_frame.pack(fill="x", pady=10)

        ctk.CTkLabel(stage_h_frame, text="Stage H JSON (ac file):",
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)

        if not hasattr(self, 'stage_m_stage_h_var'):
            self.stage_m_stage_h_var = ctk.StringVar()
        # Auto-fill if available (use last Stage H output if tracked in future)

        entry_frame_h = ctk.CTkFrame(stage_h_frame)
        entry_frame_h.pack(fill="x", padx=10, pady=5)

        stage_h_entry = ctk.CTkEntry(entry_frame_h, textvariable=self.stage_m_stage_h_var)
        stage_h_entry.pack(side="left", fill="x", expand=True, padx=5)

        ctk.CTkButton(entry_frame_h, text="Browse",
                     command=lambda: self.browse_file_for_stage(self.stage_m_stage_h_var,
                                                                 filetypes=[("JSON", "*.json")])).pack(side="right")

        # Validation indicator
        if not hasattr(self, 'stage_m_stage_h_valid'):
            self.stage_m_stage_h_valid = ctk.CTkLabel(entry_frame_h, text="", width=30)
        self.stage_m_stage_h_valid.pack(side="right", padx=5)

        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)

        if not hasattr(self, 'stage_m_process_btn'):
            self.stage_m_process_btn = ctk.CTkButton(
                process_btn_frame,
                text="Process Stage M",
                command=self.process_stage_m,
                font=ctk.CTkFont(size=16, weight="bold"),
                height=40
            )
        self.stage_m_process_btn.pack(pady=10)

        # Status Label
        if not hasattr(self, 'stage_m_status_label'):
            self.stage_m_status_label = ctk.CTkLabel(
                process_btn_frame,
                text="Ready",
                font=ctk.CTkFont(size=12),
                text_color="gray"
            )
        self.stage_m_status_label.pack(pady=5)

        # Bind validation
        self.stage_m_stage_h_var.trace('w', lambda *args: self.validate_stage_m_files())

        # Initial validation
        self.validate_stage_m_files()

    def validate_stage_m_files(self):
        """Validate Stage M input file"""
        stage_h_path = self.stage_m_stage_h_var.get().strip()

        if stage_h_path and os.path.exists(stage_h_path):
            self.stage_m_stage_h_valid.configure(text="Ok", text_color="green")
        else:
            self.stage_m_stage_h_valid.configure(text="", text_color="red")

    def process_stage_m(self):
        """Process Stage M in background thread"""
        def worker():
            try:
                self.stage_m_process_btn.configure(state="disabled", text="Processing...")
                self.update_stage_status("M", "processing")
                self.stage_m_status_label.configure(text="Processing Stage M...", text_color="blue")

                # Validate inputs
                stage_h_path = self.stage_m_stage_h_var.get().strip()

                if not stage_h_path or not os.path.exists(stage_h_path):
                    messagebox.showerror("Error", "Please select a valid Stage H JSON file")
                    return

                # Validate API keys (not strictly needed for Stage M, but keep consistent)
                if not self.api_key_manager.api_keys:
                    messagebox.showerror("Error", "Please load API keys first")
                    return

                def progress_callback(msg: str):
                    self.root.after(0, lambda: self.stage_m_status_label.configure(text=msg))

                # Detect book/chapter for status convenience (optional)
                output_path = self.stage_m_processor.process_stage_m(
                    stage_h_path=stage_h_path,
                    progress_callback=progress_callback
                )

                if output_path:
                    self.update_stage_status("M", "completed", output_path)
                    self.stage_m_status_label.configure(
                        text=f"Stage M completed successfully!\nOutput: {os.path.basename(output_path)}",
                        text_color="green"
                    )
                    messagebox.showinfo("Success", f"Stage M completed!\n\nOutput saved to:\n{output_path}")
                else:
                    self.update_stage_status("M", "error")
                    self.stage_m_status_label.configure(text="Stage M failed. Check logs for details.", text_color="red")
                    messagebox.showerror("Error", "Stage M processing failed. Check logs for details.")

            except Exception as e:
                self.logger.error(f"Error in Stage M processing: {e}", exc_info=True)
                self.update_stage_status("M", "error")
                self.stage_m_status_label.configure(text=f"Error: {str(e)}", text_color="red")
                messagebox.showerror("Error", f"Stage M processing error:\n{str(e)}")
            finally:
                self.root.after(0, lambda: self.stage_m_process_btn.configure(state="normal", text="Process Stage M"))

        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def setup_stage_l_ui(self, parent):
        """Setup UI for Stage L: Chapter Overview"""
        main_frame = ctk.CTkScrollableFrame(parent)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        title = ctk.CTkLabel(main_frame, text="Stage L: Chapter Overview", 
                            font=ctk.CTkFont(size=24, weight="bold"))
        title.pack(pady=(0, 20))
        
        desc = ctk.CTkLabel(
            main_frame,
            text="Generate per-topic chapter overview from Stage J (a file) and Stage V (b file).",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        desc.pack(pady=(0, 20))

        # Stage J File Selection
        stage_j_frame = ctk.CTkFrame(main_frame)
        stage_j_frame.pack(fill="x", pady=10)

        ctk.CTkLabel(stage_j_frame, text="Stage J JSON (a file):",
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)

        if not hasattr(self, 'stage_l_stage_j_var'):
            self.stage_l_stage_j_var = ctk.StringVar()
        # Auto-fill if available
        if hasattr(self, 'last_stage_j_path') and self.last_stage_j_path and os.path.exists(self.last_stage_j_path):
            self.stage_l_stage_j_var.set(self.last_stage_j_path)

        entry_frame_j = ctk.CTkFrame(stage_j_frame)
        entry_frame_j.pack(fill="x", padx=10, pady=5)

        stage_j_entry = ctk.CTkEntry(entry_frame_j, textvariable=self.stage_l_stage_j_var)
        stage_j_entry.pack(side="left", fill="x", expand=True, padx=5)

        ctk.CTkButton(
            entry_frame_j,
            text="Browse",
            command=lambda: self.browse_file_for_stage(self.stage_l_stage_j_var, filetypes=[("JSON", "*.json")])
        ).pack(side="right")

        # Validation indicator for Stage J file
        if not hasattr(self, 'stage_l_stage_j_valid'):
            self.stage_l_stage_j_valid = ctk.CTkLabel(entry_frame_j, text="", width=30)
        self.stage_l_stage_j_valid.pack(side="right", padx=5)

        # Stage V File Selection
        stage_v_frame = ctk.CTkFrame(main_frame)
        stage_v_frame.pack(fill="x", pady=10)

        ctk.CTkLabel(stage_v_frame, text="Stage V JSON (b file):",
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)

        if not hasattr(self, 'stage_l_stage_v_var'):
            self.stage_l_stage_v_var = ctk.StringVar()

        entry_frame_v = ctk.CTkFrame(stage_v_frame)
        entry_frame_v.pack(fill="x", padx=10, pady=5)

        stage_v_entry = ctk.CTkEntry(entry_frame_v, textvariable=self.stage_l_stage_v_var)
        stage_v_entry.pack(side="left", fill="x", expand=True, padx=5)

        ctk.CTkButton(
            entry_frame_v,
            text="Browse",
            command=lambda: self.browse_file_for_stage(self.stage_l_stage_v_var, filetypes=[("JSON", "*.json")])
        ).pack(side="right")

        # Validation indicator for Stage V file
        if not hasattr(self, 'stage_l_stage_v_valid'):
            self.stage_l_stage_v_valid = ctk.CTkLabel(entry_frame_v, text="", width=30)
        self.stage_l_stage_v_valid.pack(side="right", padx=5)

        # Prompt Section
        prompt_frame = ctk.CTkFrame(main_frame)
        prompt_frame.pack(fill="x", pady=10)

        ctk.CTkLabel(
            prompt_frame,
            text="Prompt for Chapter Overview:",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=5)

        if not hasattr(self, "stage_l_prompt_text"):
            self.stage_l_prompt_text = ctk.CTkTextbox(prompt_frame, height=150, font=self.farsi_text_font)
        self.stage_l_prompt_text.pack(fill="x", padx=10, pady=5)

        # Model Selection
        model_frame = ctk.CTkFrame(main_frame)
        model_frame.pack(fill="x", pady=10)

        ctk.CTkLabel(
            model_frame,
            text="Model for Stage L:",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=5)

        default_model = "gemini-2.5-flash"
        if hasattr(self, "model_var") and self.model_var:
            default_model = self.model_var.get()

        if not hasattr(self, "stage_l_model_var"):
            self.stage_l_model_var = ctk.StringVar(value=default_model)
        if not hasattr(self, "stage_l_model_combo"):
            self.stage_l_model_combo = ctk.CTkComboBox(
                model_frame,
                values=APIConfig.TEXT_MODELS,
                variable=self.stage_l_model_var,
                width=300
            )
        self.stage_l_model_combo.pack(anchor="w", padx=10, pady=5)

        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)

        if not hasattr(self, 'stage_l_process_btn'):
            self.stage_l_process_btn = ctk.CTkButton(
                process_btn_frame,
                text="Process Stage L",
                command=self.process_stage_l,
                font=ctk.CTkFont(size=16, weight="bold"),
                height=40
            )
        self.stage_l_process_btn.pack(pady=10)

        # Status Label
        if not hasattr(self, 'stage_l_status_label'):
            self.stage_l_status_label = ctk.CTkLabel(
                process_btn_frame,
                text="Ready",
                font=ctk.CTkFont(size=12),
                text_color="gray"
            )
        self.stage_l_status_label.pack(pady=5)

        # Bind validation
        self.stage_l_stage_j_var.trace('w', lambda *args: self.validate_stage_l_files())
        self.stage_l_stage_v_var.trace('w', lambda *args: self.validate_stage_l_files())

        # Initial validation
        self.validate_stage_l_files()

    def validate_stage_l_files(self):
        """Validate Stage L input files"""
        stage_j_path = self.stage_l_stage_j_var.get().strip()
        stage_v_path = self.stage_l_stage_v_var.get().strip()

        # Stage J
        if stage_j_path and os.path.exists(stage_j_path):
            self.stage_l_stage_j_valid.configure(text="Ok", text_color="green")
        else:
            self.stage_l_stage_j_valid.configure(text="", text_color="red")

        # Stage V
        if stage_v_path and os.path.exists(stage_v_path):
            self.stage_l_stage_v_valid.configure(text="Ok", text_color="green")
        else:
            self.stage_l_stage_v_valid.configure(text="", text_color="red")

    def process_stage_l(self):
        """Process Stage L in background thread"""
        def worker():
            try:
                self.stage_l_process_btn.configure(state="disabled", text="Processing...")
                self.update_stage_status("L", "processing")
                self.stage_l_status_label.configure(text="Processing Stage L...", text_color="blue")

                # Validate inputs
                stage_j_path = self.stage_l_stage_j_var.get().strip()
                stage_v_path = self.stage_l_stage_v_var.get().strip()

                if not stage_j_path or not os.path.exists(stage_j_path):
                    messagebox.showerror("Error", "Please select a valid Stage J JSON file (a file)")
                    return

                if not stage_v_path or not os.path.exists(stage_v_path):
                    messagebox.showerror("Error", "Please select a valid Stage V JSON file (b file)")
                    return

                prompt = self.stage_l_prompt_text.get("1.0", tk.END).strip()
                model_name = self.stage_l_model_var.get()

                if not prompt:
                    messagebox.showerror("Error", "Please enter a prompt for Stage L")
                    return

                # Validate API keys (to be consistent with other stages)
                if not self.api_key_manager.api_keys:
                    messagebox.showerror("Error", "Please load API keys first")
                    return

                def progress_callback(msg: str):
                    self.root.after(0, lambda: self.stage_l_status_label.configure(text=msg))

                # Process Stage L
                output_path = self.stage_l_processor.process_stage_l(
                    stage_j_path=stage_j_path,
                    stage_v_path=stage_v_path,
                    prompt=prompt,
                    model_name=model_name,
                    progress_callback=progress_callback
                )

                if output_path:
                    self.update_stage_status("L", "completed", output_path)
                    self.stage_l_status_label.configure(
                        text=f"Stage L completed successfully!\nOutput: {os.path.basename(output_path)}",
                        text_color="green"
                    )
                    messagebox.showinfo("Success", f"Stage L completed!\n\nOutput saved to:\n{output_path}")
                else:
                    self.update_stage_status("L", "error")
                    self.stage_l_status_label.configure(text="Stage L failed. Check logs for details.", text_color="red")
                    messagebox.showerror("Error", "Stage L processing failed. Check logs for details.")

            except Exception as e:
                self.logger.error(f"Error in Stage L processing: {e}", exc_info=True)
                self.update_stage_status("L", "error")
                self.stage_l_status_label.configure(text=f"Error: {str(e)}", text_color="red")
                messagebox.showerror("Error", f"Stage L processing error:\n{str(e)}")
            finally:
                self.root.after(0, lambda: self.stage_l_process_btn.configure(state="normal", text="Process Stage L"))

        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def run(self):
        """Start the GUI main loop"""
        self.root.mainloop()


def main():
    """Main entry point for the application"""
    app = ContentAutomationGUI()
    app.run()


if __name__ == "__main__":
    main()
