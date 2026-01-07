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
import time
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
from stage_x_processor import StageXProcessor
from stage_y_processor import StageYProcessor
from stage_z_processor import StageZProcessor
from pre_ocr_topic_processor import PreOCRTopicProcessor
from automated_pipeline_orchestrator import AutomatedPipelineOrchestrator


class ContentAutomationGUI:
    """Main GUI application for content automation"""
    
    def __init__(self):
        # Configure appearance
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        
        # Initialize main window
        self.root = ctk.CTk()
        self.root.title("Content Automation - Pileh")
        self.root.geometry("1000x800")
        self.root.minsize(1024, 768)

        # Common Farsi-friendly font for textboxes
        try:
            self.farsi_text_font = ctk.CTkFont(family="Tahoma", size=13)
        except Exception:
            # Fallback if Tahoma is not available
            self.farsi_text_font = ctk.CTkFont(size=14)
        
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
        self.stage_x_processor = StageXProcessor(self.api_client)
        self.stage_y_processor = StageYProcessor(self.api_client)
        self.stage_z_processor = StageZProcessor(self.api_client)
        self.pre_ocr_topic_processor = PreOCRTopicProcessor(self.api_client)
        
        # Variables
        self.pdf_path = None
        self.selected_prompt_name = None
        self.custom_prompt = ""
        
        # Default settings from main view (will be used across all stages)
        # These are set from Model Selection and Output Section in main view
        self.use_custom_prompt = False
        self.last_final_output_path = None       # Stage 1 JSON
        self.last_post_processed_path = None     # Stage 2 JSON
        self.last_corrected_path = None          # Stage 3 JSON (with PointId)
        self.last_stage3_raw_path = None         # Stage 3 raw JSON (new intermediate stage)
        self.last_stage4_raw_path = None         # Stage 4 raw JSON (chunked model output)
        self.last_stage_e_path = None            # Stage E JSON
        self.last_stage_j_path = None            # Stage J JSON
        self.last_stage_f_path = None            # Stage F JSON
        self.last_stage_h_path = None            # Stage H JSON
        self.last_stage_v_path = None            # Stage V JSON
        self.last_stage_m_path = None            # Stage M JSON
        self.last_stage_l_path = None            # Stage L JSON
        
        # Initialize default settings variables (shared across all views)
        # These will be used as default values for all stages
        self.model_var = ctk.StringVar(value="gemini-2.5-pro")
        self.output_folder_var = ctk.StringVar()
        
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
        
        # Tab 1: Pre-OCR Topic Extraction (FIRST TAB)
        self.tab_pre_ocr_topic = self.main_tabview.add("Pre-OCR Topic Extraction")
        self.setup_pre_ocr_topic_ui(self.tab_pre_ocr_topic)
        
        # Tab 2: OCR Extraction
        self.tab_ocr_extraction = self.main_tabview.add("OCR Extraction")
        self.setup_ocr_extraction_ui(self.tab_ocr_extraction)
        
        # Tab 3: Document Processing (for tabview access)
        self.tab_stages_1_4 = self.main_tabview.add("Document Processing")
        
        # Main page: Stages 1-4 (Original UI) - shown directly on root
        # This is the Part 1 form (Stage 1 only)
        self.main_stages_1_4_frame = ctk.CTkFrame(self.root)
        self.main_stages_1_4_frame.pack(fill="both", expand=True, padx=10, pady=10)
        self.setup_stages_1_4_ui(self.main_stages_1_4_frame)
        
        # Now setup Stages 2-3-4 UI in tabview (the actual Stages 1-4 form)
        self.setup_stages_2_3_4_ui(self.tab_stages_1_4)
        
        # Tab 3: Image Notes Generation
        self.tab_stage_e = self.main_tabview.add("Image Notes Generation")
        self.setup_stage_e_ui(self.tab_stage_e)
        
        # Tab 3: Image File Catalog
        self.tab_stage_f = self.main_tabview.add("Image File Catalog")
        self.setup_stage_f_ui(self.tab_stage_f)
        
        # Tab 4: Importance & Type Tagging
        self.tab_stage_j = self.main_tabview.add("Importance & Type Tagging")
        self.setup_stage_j_ui(self.tab_stage_j)
        
        # Tab 5: Flashcard Generation
        self.tab_stage_h = self.main_tabview.add("Flashcard Generation")
        self.setup_stage_h_ui(self.tab_stage_h)
        
        # Tab 6: Test Bank Generation
        self.tab_stage_v = self.main_tabview.add("Test Bank Generation")
        self.setup_stage_v_ui(self.tab_stage_v)
        
        # Tab 7: Topic List Extraction
        self.tab_stage_m = self.main_tabview.add("Topic List Extraction")
        self.setup_stage_m_ui(self.tab_stage_m)
        
        # Tab 8: Chapter Summary
        self.tab_stage_l = self.main_tabview.add("Chapter Summary")
        self.setup_stage_l_ui(self.tab_stage_l)
        
        # Tab 9: Book Changes Detection
        self.tab_stage_x = self.main_tabview.add("Book Changes Detection")
        self.setup_stage_x_ui(self.tab_stage_x)
        
        # Tab 10: Deletion Detection
        self.tab_stage_y = self.main_tabview.add("Deletion Detection")
        self.setup_stage_y_ui(self.tab_stage_y)
        
        # Tab 11: RichText Generation
        self.tab_stage_z = self.main_tabview.add("RichText Generation")
        self.setup_stage_z_ui(self.tab_stage_z)
        
        # Tab 12: JSON to CSV Converter
        self.tab_json_to_csv = self.main_tabview.add("JSON to CSV Converter")
        self.setup_json_to_csv_ui(self.tab_json_to_csv)
        
        # Ensure Pre-OCR Topic Extraction is the default/selected tab
        self.main_tabview.set("Pre-OCR Topic Extraction")
        
        # Pipeline Status Bar (shown on all tabs)
        self.setup_pipeline_status_bar()
    
    def setup_stages_1_4_ui(self, parent):
        """Setup UI for Document Processing (original functionality)"""
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
        
        # Title for main view only
        if not is_tabview:
            title_label = ctk.CTkLabel(
                main_frame,
                text="Settings",
                font=ctk.CTkFont(size=24, weight="bold")
            )
            title_label.pack(pady=(0, 20))
        
        # Setup all sections - widgets will be shared via StringVar and other variables
        # API Configuration Section
        self.setup_api_section(main_frame)
        
        # Model Selection Section
        self.setup_model_section(main_frame)
        
        # Output Section
        self.setup_output_section(main_frame)
        
        # Automated Pipeline Section (includes View Other Tools button)
        self.setup_automated_pipeline_section(main_frame)
        
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
    
    def setup_pre_ocr_topic_ui(self, parent):
        """Setup UI for Pre-OCR Topic Extraction"""
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
        title = ctk.CTkLabel(main_frame, text="Pre-OCR Topic Extraction", 
                            font=ctk.CTkFont(size=24, weight="bold"))
        title.pack(pady=(0, 20))
        
        # Description
        desc = ctk.CTkLabel(
            main_frame, 
            text="Extract topics from PDF before OCR extraction. Output: t{book}{chapter}_{pdf_name}.json",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        desc.pack(pady=(0, 20))
        
        # Multiple PDF files selection for batch processing
        multi_pdf_frame = ctk.CTkFrame(main_frame)
        multi_pdf_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(multi_pdf_frame, text="Multiple PDF Files (Batch Processing):", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        multi_file_btn_frame = ctk.CTkFrame(multi_pdf_frame)
        multi_file_btn_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkButton(multi_file_btn_frame, text="Browse Multiple Files", 
                     command=self.browse_multiple_pre_ocr_pdfs, 
                     width=150).pack(side="left", padx=5)
        
        # Delay setting
        delay_frame = ctk.CTkFrame(multi_file_btn_frame)
        delay_frame.pack(side="left", padx=10)
        ctk.CTkLabel(delay_frame, text="Delay (seconds):").pack(side="left", padx=5)
        if not hasattr(self, 'pre_ocr_delay_var'):
            self.pre_ocr_delay_var = ctk.StringVar(value="5")
        delay_entry = ctk.CTkEntry(delay_frame, textvariable=self.pre_ocr_delay_var, width=60)
        delay_entry.pack(side="left", padx=5)
        
        # File list with status
        if not hasattr(self, 'pre_ocr_file_list_frame'):
            self.pre_ocr_file_list_frame = ctk.CTkFrame(main_frame)
        self.pre_ocr_file_list_frame.pack(fill="both", expand=True, pady=10)
        
        ctk.CTkLabel(self.pre_ocr_file_list_frame, text="Selected Files:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        # Scrollable frame for file list
        if not hasattr(self, 'pre_ocr_file_list_scroll'):
            self.pre_ocr_file_list_scroll = ctk.CTkScrollableFrame(self.pre_ocr_file_list_frame, height=200)
        self.pre_ocr_file_list_scroll.pack(fill="both", expand=True, padx=10, pady=5)
        
        # Progress bar
        if not hasattr(self, 'pre_ocr_progress_bar'):
            self.pre_ocr_progress_bar = ctk.CTkProgressBar(main_frame)
        self.pre_ocr_progress_bar.pack(fill="x", padx=10, pady=5)
        self.pre_ocr_progress_bar.set(0)
        
        # Progress label
        if not hasattr(self, 'pre_ocr_progress_label'):
            self.pre_ocr_progress_label = ctk.CTkLabel(main_frame, text="Ready", 
                                                      font=ctk.CTkFont(size=12))
        self.pre_ocr_progress_label.pack(pady=5)
        
        # Variables for file tracking
        if not hasattr(self, 'pre_ocr_selected_files'):
            self.pre_ocr_selected_files = []  # List of dicts: {'path': str, 'status_label': widget, 'frame': widget, 'output_path': str}
        
        # Prompt selection for Pre-OCR
        prompt_frame = ctk.CTkFrame(main_frame)
        prompt_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(prompt_frame, text="Prompt for Topic Extraction:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'pre_ocr_prompt_text'):
            self.pre_ocr_prompt_text = ctk.CTkTextbox(prompt_frame, height=120, font=self.farsi_text_font)
        self.pre_ocr_prompt_text.pack(fill="x", padx=10, pady=(0, 10))
        
        # Load default prompt from prompts.json
        default_prompt = self.prompt_manager.get_prompt("Pre OCR Topic") or ""
        if default_prompt and not self.pre_ocr_prompt_text.get("1.0", tk.END).strip():
            self.pre_ocr_prompt_text.delete("1.0", tk.END)
            self.pre_ocr_prompt_text.insert("1.0", default_prompt)
        
        # Model Selection
        model_frame = ctk.CTkFrame(main_frame)
        model_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(model_frame, text="Select Gemini Model:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'pre_ocr_model_var'):
            self.pre_ocr_model_var = ctk.StringVar(value="gemini-2.5-pro")
        pre_ocr_model_combo = ctk.CTkComboBox(model_frame, values=APIConfig.TEXT_MODELS, 
                                             variable=self.pre_ocr_model_var, width=400)
        pre_ocr_model_combo.pack(anchor="w", padx=10, pady=(0, 10))
        
        # Process button
        process_frame = ctk.CTkFrame(main_frame)
        process_frame.pack(fill="x", pady=10)
        
        # Process All button for batch processing
        if not hasattr(self, 'pre_ocr_process_all_btn'):
            self.pre_ocr_process_all_btn = ctk.CTkButton(process_frame, text="Process All Files", 
                                                        command=self.process_multiple_pre_ocr_topics, 
                                                        width=200, height=40,
                                                        font=ctk.CTkFont(size=14, weight="bold"),
                                                        fg_color="green", hover_color="darkgreen")
        self.pre_ocr_process_all_btn.pack(side="left", padx=10, pady=10)
        
        # Status label
        if not hasattr(self, 'pre_ocr_status_label'):
            self.pre_ocr_status_label = ctk.CTkLabel(process_frame, text="Ready", 
                                                     font=ctk.CTkFont(size=12), text_color="gray")
        self.pre_ocr_status_label.pack(side="left", padx=10, pady=10)
        
        # Output file path
        output_frame = ctk.CTkFrame(main_frame)
        output_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(output_frame, text="Output File (t{book}{chapter}_{pdf_name}.json):", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'pre_ocr_output_var'):
            self.pre_ocr_output_var = ctk.StringVar()
        pre_ocr_output_entry = ctk.CTkEntry(output_frame, textvariable=self.pre_ocr_output_var, 
                                                width=400, state="readonly")
        pre_ocr_output_entry.pack(fill="x", padx=10, pady=(0, 10))
    
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
        # Don't pack yet - will be packed by on_prompt_type_change
        
        # Custom prompt input
        self.custom_frame = ctk.CTkFrame(prompt_frame)
        
        ctk.CTkLabel(self.custom_frame, text="Enter Custom Prompt:", 
                    font=ctk.CTkFont(size=12, weight="bold")).pack(anchor="w", padx=10, pady=(10, 5))
        
        self.custom_prompt_text = ctk.CTkTextbox(self.custom_frame, height=120, font=self.farsi_text_font)
        self.custom_prompt_text.pack(fill="x", padx=10, pady=(0, 10))
        
        # Initialize use_custom_prompt flag
        self.use_custom_prompt = False
        
        # Update visibility based on initial selection (this will pack/unpack widgets)
        self.on_prompt_type_change()
        
        # Load default prompt into preview after visibility is set
        if prompt_names and default_value:
            self.on_prompt_selected(default_value)
    
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
        
        # Use shared model_var (initialized in __init__)
        self.model_combo = ctk.CTkComboBox(model_select_frame, values=APIConfig.TEXT_MODELS, 
                                          variable=self.model_var, width=400,
                                          command=self.on_model_changed)
        self.model_combo.pack(anchor="w", padx=10, pady=(0, 10))
        
        # Also add trace to StringVar to catch any changes (using trace like other variables)
        self.model_var.trace('w', lambda *args: self._on_model_var_changed())
        
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
        
        # Use shared output_folder_var (initialized in __init__)
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
            text="Start Per Stage Processing",
            command=self.show_tabview,
            width=200,
            height=40,
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color="purple",
            hover_color="darkviolet"
        )
        self.show_tabview_btn.pack(side="left", padx=10)
        
    
    def setup_automated_pipeline_section(self, parent):
        """Setup Automated Pipeline section in main page"""
        pipeline_frame = ctk.CTkFrame(parent)
        pipeline_frame.pack(fill="x", pady=(0, 20))
        
        # Title
        title_label = ctk.CTkLabel(
            pipeline_frame,
            text="Automated Pipeline",
            font=ctk.CTkFont(size=18, weight="bold")
        )
        title_label.pack(pady=(15, 10))
        
        desc_label = ctk.CTkLabel(
            pipeline_frame,
            text="Run complete pipeline automatically: PDF -> Stage 1 -> Stage 2 -> Stage 3 -> Stage 4 -> Stage E -> Stage F -> Stage J -> Stage V -> Stage X -> Stage Y -> Stage Z\n"
                 "If Word file is not provided, Stage J and V will fail but pipeline continues.\n"
                 "If Old Book PDF is not provided, Stage X, Y, and Z will be skipped.",
            font=ctk.CTkFont(size=11),
            justify="left",
            text_color="gray"
        )
        desc_label.pack(pady=(0, 15))
        
        # PointID input
        pointid_frame = ctk.CTkFrame(pipeline_frame)
        pointid_frame.pack(fill="x", padx=15, pady=5)
        ctk.CTkLabel(pointid_frame, text="Start PointID (10 digits):", width=180).pack(side="left", padx=5)
        self.auto_pipeline_pointid_var = ctk.StringVar(value="1050030001")
        ctk.CTkEntry(pointid_frame, textvariable=self.auto_pipeline_pointid_var, width=200).pack(side="left", padx=5)
        ctk.CTkLabel(pointid_frame, text="Example: 1050030001", font=ctk.CTkFont(size=10), text_color="gray").pack(side="left", padx=5)
        
        # Chapter name
        chapter_frame = ctk.CTkFrame(pipeline_frame)
        chapter_frame.pack(fill="x", padx=15, pady=5)
        ctk.CTkLabel(chapter_frame, text="Chapter Name:", width=180).pack(side="left", padx=5)
        self.auto_pipeline_chapter_var = ctk.StringVar(value="")
        ctk.CTkEntry(chapter_frame, textvariable=self.auto_pipeline_chapter_var, width=400).pack(side="left", padx=5, fill="x", expand=True)
        
        # Word file (optional)
        word_frame = ctk.CTkFrame(pipeline_frame)
        word_frame.pack(fill="x", padx=15, pady=5)
        ctk.CTkLabel(word_frame, text="Word File (for Stage J & V):", width=180).pack(side="left", padx=5)
        self.auto_pipeline_word_var = ctk.StringVar(value="")
        ctk.CTkEntry(word_frame, textvariable=self.auto_pipeline_word_var, width=300).pack(side="left", padx=5, fill="x", expand=True)
        ctk.CTkButton(
            word_frame,
            text="Browse",
            width=100,
            command=lambda: self.auto_pipeline_word_var.set(filedialog.askopenfilename(filetypes=[("Word files", "*.docx *.doc"), ("All files", "*.*")]))
        ).pack(side="left", padx=5)
        ctk.CTkLabel(
            word_frame,
            text="(Optional - required for Stage J and V)",
            font=ctk.CTkFont(size=10),
            text_color="orange"
        ).pack(side="left", padx=5)
        
        # Old Book PDF (for Stage X)
        old_pdf_frame = ctk.CTkFrame(pipeline_frame)
        old_pdf_frame.pack(fill="x", padx=15, pady=5)
        ctk.CTkLabel(old_pdf_frame, text="Old Book PDF (for Stage X):", width=180).pack(side="left", padx=5)
        self.auto_pipeline_old_pdf_var = ctk.StringVar(value="")
        ctk.CTkEntry(old_pdf_frame, textvariable=self.auto_pipeline_old_pdf_var, width=300).pack(side="left", padx=5, fill="x", expand=True)
        ctk.CTkButton(
            old_pdf_frame,
            text="Browse",
            width=100,
            command=lambda: self.auto_pipeline_old_pdf_var.set(filedialog.askopenfilename(filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")]))
        ).pack(side="left", padx=5)
        ctk.CTkLabel(
            old_pdf_frame,
            text="(Optional - required for Stage X, Y, Z)",
            font=ctk.CTkFont(size=10),
            text_color="orange"
        ).pack(side="left", padx=5)
        
        # Progress label
        self.auto_pipeline_progress_label = ctk.CTkLabel(
            pipeline_frame,
            text="Ready to start automated pipeline...",
            font=ctk.CTkFont(size=11),
            text_color="gray"
        )
        self.auto_pipeline_progress_label.pack(pady=(10, 5))
        
        # Buttons frame (Start Pipeline and View Other Tools)
        buttons_frame = ctk.CTkFrame(pipeline_frame)
        buttons_frame.pack(pady=(5, 15))
        
        # Start button
        self.auto_pipeline_start_btn = ctk.CTkButton(
            buttons_frame,
            text="Start Automated Pipeline",
            command=self.start_automated_pipeline_from_main,
            width=300,
            height=45,
            font=ctk.CTkFont(size=15, weight="bold"),
            fg_color="orange",
            hover_color="darkorange"
        )
        self.auto_pipeline_start_btn.pack(side="left", padx=10)
        
        # View Other Tools button (moved from setup_controls)
        self.show_tabview_btn = ctk.CTkButton(
            buttons_frame,
            text="Start Per Stage Processing",
            command=self.show_tabview,
            width=200,
            height=45,
            font=ctk.CTkFont(size=15, weight="bold"),
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
    
    
    def browse_multiple_pre_ocr_pdfs(self):
        """Browse and select multiple PDF files"""
        file_paths = filedialog.askopenfilenames(
            title="Select PDF Files for Pre-OCR Topic Extraction",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")]
        )
        
        if file_paths:
            self.pre_ocr_selected_files = []
            # Clear existing widgets
            for widget in self.pre_ocr_file_list_scroll.winfo_children():
                widget.destroy()
            
            # Add each file to the list
            for file_path in file_paths:
                file_frame = ctk.CTkFrame(self.pre_ocr_file_list_scroll)
                file_frame.pack(fill="x", padx=5, pady=2)
                
                # File name
                file_name = os.path.basename(file_path)
                name_label = ctk.CTkLabel(file_frame, text=file_name, 
                                         font=ctk.CTkFont(size=11))
                name_label.pack(side="left", padx=5)
                
                # Status label
                status_label = ctk.CTkLabel(file_frame, text="Pending", 
                                          text_color="gray", font=ctk.CTkFont(size=11))
                status_label.pack(side="right", padx=5)
                
                # Remove button
                remove_btn = ctk.CTkButton(file_frame, text="X", width=30, height=20,
                                          command=lambda f=file_path, w=file_frame: self.remove_pre_ocr_file(f, w))
                remove_btn.pack(side="right", padx=2)
                
                self.pre_ocr_selected_files.append({
                    'path': file_path,
                    'status_label': status_label,
                    'frame': file_frame,
                    'output_path': None
                })
            
            self.logger.info(f"Selected {len(self.pre_ocr_selected_files)} PDF files for batch processing")
    
    def remove_pre_ocr_file(self, file_path, frame_widget):
        """Remove a file from the selection list"""
        self.pre_ocr_selected_files = [f for f in self.pre_ocr_selected_files if f['path'] != file_path]
        frame_widget.destroy()
    
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
            self.logger.info(f"Default output folder updated to: {folder}")
    
    def on_model_changed(self, value: str):
        """Callback when model is changed in ComboBox"""
        if value:
            value = value.strip()
            # Force update StringVar to ensure it's synced
            self.model_var.set(value)
            # Verify the update
            verify_value = self.model_var.get()
            self.logger.info(f"[on_model_changed] Model changed to: {value}, StringVar now: {verify_value}")
            if verify_value != value:
                self.logger.warning(f"[on_model_changed] WARNING: StringVar sync failed! Expected: {value}, Got: {verify_value}")
            # Sync all model ComboBoxes with the new value
            self.sync_all_model_combos(value)
            # Also update status if update_status method exists
            try:
                self.update_status(f"Default model set to: {value}")
            except:
                pass
    
    def _on_model_var_changed(self):
        """Callback when model_var StringVar is changed"""
        try:
            new_value = self.model_var.get()
            if new_value:
                self.logger.info(f"[_on_model_var_changed] Model variable changed to: {new_value}")
                # Sync all model ComboBoxes with the new value
                self.sync_all_model_combos(new_value)
        except Exception as e:
            self.logger.warning(f"Error in _on_model_var_changed: {e}")
    
    def sync_all_model_combos(self, model_value: str):
        """Sync all model ComboBoxes in all stages with the default model from settings"""
        if not model_value:
            return
        
        model_value = model_value.strip()
        synced_count = 0
        
        # List of all model variables to sync
        model_vars_to_sync = [
            'pre_ocr_model_var',
            'ocr_extraction_model_var',
            'stage_e_model_var',
            'stage_j_model_var',
            'stage_h_model_var',
            'stage_v_model1_var',
            'stage_v_model2_var',
            'stage_l_model_var',
            'stage_x_pdf_model_var',
            'stage_x_change_model_var',
            'stage_y_ocr_model_var',
            # Note: stage_y_deletion_model_var is not in UI, deletion_detection_model uses get_default_model()
            'stage_z_model_var',
            'second_stage_model_var',
            'auto_stage3_model_var',
            'auto_stage4_model_var',
        ]
        
        # Update all model variables
        for var_name in model_vars_to_sync:
            if hasattr(self, var_name):
                try:
                    var = getattr(self, var_name)
                    if isinstance(var, ctk.StringVar):
                        current_value = var.get()
                        if current_value != model_value:
                            var.set(model_value)
                            synced_count += 1
                            self.logger.debug(f"Synced {var_name} to {model_value}")
                except Exception as e:
                    self.logger.warning(f"Failed to sync {var_name}: {e}")
        
        if synced_count > 0:
            self.logger.info(f"[sync_all_model_combos] Synced {synced_count} model ComboBoxes to: {model_value}")
    
    def get_default_model(self) -> str:
        """Get default model from main view Model Selection"""
        # Try both ComboBox and StringVar, prefer the one that has a value
        combo_value = None
        var_value = None
        
        # Get from ComboBox
        if hasattr(self, 'model_combo'):
            try:
                combo_value = self.model_combo.get()
                if combo_value:
                    combo_value = combo_value.strip()
            except Exception as e:
                self.logger.warning(f"Error getting model from ComboBox: {e}")
        
        # Get from StringVar
        if hasattr(self, 'model_var'):
            try:
                var_value = self.model_var.get()
                if var_value:
                    var_value = var_value.strip()
            except Exception as e:
                self.logger.warning(f"Error getting model from StringVar: {e}")
        
        # Log what we found for debugging
        self.logger.info(f"[get_default_model] ComboBox value: {combo_value}, StringVar value: {var_value}")
        
        # Prefer ComboBox value (most up-to-date), but sync both
        if combo_value:
            # Sync StringVar with ComboBox to keep them in sync
            if hasattr(self, 'model_var'):
                current_var = self.model_var.get()
                if current_var != combo_value:
                    self.model_var.set(combo_value)
                    self.logger.info(f"[get_default_model] Synced StringVar with ComboBox: {combo_value}")
            self.logger.info(f"[get_default_model] Returning model from ComboBox: {combo_value}")
            return combo_value
        
        # Fallback to StringVar
        if var_value:
            # Sync ComboBox with StringVar if possible
            if hasattr(self, 'model_combo'):
                try:
                    current_combo = self.model_combo.get()
                    if current_combo != var_value:
                        self.model_combo.set(var_value)
                        self.logger.info(f"[get_default_model] Synced ComboBox with StringVar: {var_value}")
                except Exception as e:
                    self.logger.warning(f"[get_default_model] Could not sync ComboBox: {e}")
            self.logger.info(f"[get_default_model] Returning model from StringVar: {var_value}")
            return var_value
        
        # Final fallback
        fallback = "gemini-2.5-pro"
        self.logger.warning(f"[get_default_model] Both ComboBox and StringVar are empty, using fallback: {fallback}")
        return fallback
    
    def get_default_output_dir(self, fallback_path: Optional[str] = None) -> str:
        """Get default output directory from main view Output Section"""
        # Always get current value from output_folder_var (shared across all views)
        if hasattr(self, 'output_folder_var'):
            current_value = self.output_folder_var.get()
            if current_value:
                output_dir = current_value.strip()
                if output_dir and os.path.exists(output_dir):
                    self.logger.debug(f"Using default output dir from settings: {output_dir}")
                    return output_dir
        
        # Fallback to provided path (e.g., PDF directory)
        if fallback_path:
            fallback_dir = os.path.dirname(fallback_path) if os.path.isfile(fallback_path) else fallback_path
            if fallback_dir and os.path.exists(fallback_dir):
                self.logger.debug(f"Using fallback output dir from path: {fallback_dir}")
                return fallback_dir
        
        # Final fallback to current working directory
        cwd = os.getcwd()
        self.logger.debug(f"Using current working directory as output dir: {cwd}")
        return cwd
    
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

    def on_second_stage_default_prompt_selected(self, selected_name: str):
        """When default Stage 2 prompt combobox changes, fill the Stage 2 prompt textbox."""
        prompt_text = self.prompt_manager.get_prompt(selected_name)
        if prompt_text and hasattr(self, 'second_stage_prompt_text'):
            # Ensure textbox is writable for programmatic updates
            try:
                self.second_stage_prompt_text.configure(state="normal")
            except Exception:
                pass
            self.second_stage_prompt_text.delete("1.0", tk.END)
            self.second_stage_prompt_text.insert("1.0", prompt_text)
            # Restore disabled state if we are in default mode
            if hasattr(self, 'second_stage_prompt_type_var') and self.second_stage_prompt_type_var.get() == "default":
                try:
                    self.second_stage_prompt_text.configure(state="disabled")
                except Exception:
                    pass

    def on_auto_stage3_default_prompt_selected(self, selected_name: str):
        """When default Stage 3 prompt combobox changes, fill the Stage 3 prompt textbox."""
        prompt_text = self.prompt_manager.get_prompt(selected_name)
        if prompt_text and hasattr(self, 'auto_stage3_prompt_text'):
            try:
                self.auto_stage3_prompt_text.configure(state="normal")
            except Exception:
                pass
            self.auto_stage3_prompt_text.delete("1.0", tk.END)
            self.auto_stage3_prompt_text.insert("1.0", prompt_text)
            if hasattr(self, 'auto_stage3_prompt_type_var') and self.auto_stage3_prompt_type_var.get() == "default":
                try:
                    self.auto_stage3_prompt_text.configure(state="disabled")
                except Exception:
                    pass

    def on_auto_stage4_default_prompt_selected(self, selected_name: str):
        """When default Stage 4 prompt combobox changes, fill the Stage 4 prompt textbox."""
        prompt_text = self.prompt_manager.get_prompt(selected_name)
        if prompt_text and hasattr(self, 'auto_stage4_prompt_text'):
            try:
                self.auto_stage4_prompt_text.configure(state="normal")
            except Exception:
                pass
            self.auto_stage4_prompt_text.delete("1.0", tk.END)
            self.auto_stage4_prompt_text.insert("1.0", prompt_text)
            if hasattr(self, 'auto_stage4_prompt_type_var') and self.auto_stage4_prompt_type_var.get() == "default":
                try:
                    self.auto_stage4_prompt_text.configure(state="disabled")
                except Exception:
                    pass

    def on_stage_e_default_prompt_selected(self, selected_name: str):
        """When default Stage E prompt combobox changes, fill the Stage E prompt textbox."""
        prompt_text = self.prompt_manager.get_prompt(selected_name)
        if prompt_text and hasattr(self, 'stage_e_prompt_text'):
            try:
                self.stage_e_prompt_text.configure(state="normal")
            except Exception:
                pass
            self.stage_e_prompt_text.delete("1.0", tk.END)
            self.stage_e_prompt_text.insert("1.0", prompt_text)
            if hasattr(self, 'stage_e_prompt_type_var') and self.stage_e_prompt_type_var.get() == "default":
                try:
                    self.stage_e_prompt_text.configure(state="disabled")
                except Exception:
                    pass

    def on_stage_j_default_prompt_selected(self, selected_name: str):
        """When default Stage J prompt combobox changes, fill the Stage J prompt textbox."""
        prompt_text = self.prompt_manager.get_prompt(selected_name)
        if prompt_text and hasattr(self, 'stage_j_prompt_text'):
            try:
                self.stage_j_prompt_text.configure(state="normal")
            except Exception:
                pass
            self.stage_j_prompt_text.delete("1.0", tk.END)
            self.stage_j_prompt_text.insert("1.0", prompt_text)
            if hasattr(self, 'stage_j_prompt_type_var') and self.stage_j_prompt_type_var.get() == "default":
                try:
                    self.stage_j_prompt_text.configure(state="disabled")
                except Exception:
                    pass

    def on_stage_h_default_prompt_selected(self, selected_name: str):
        """When default Stage H prompt combobox changes, fill the Stage H prompt textbox."""
        prompt_text = self.prompt_manager.get_prompt(selected_name)
        if prompt_text and hasattr(self, 'stage_h_prompt_text'):
            try:
                self.stage_h_prompt_text.configure(state="normal")
            except Exception:
                pass
            self.stage_h_prompt_text.delete("1.0", tk.END)
            self.stage_h_prompt_text.insert("1.0", prompt_text)
            if hasattr(self, 'stage_h_prompt_type_var') and self.stage_h_prompt_type_var.get() == "default":
                try:
                    self.stage_h_prompt_text.configure(state="disabled")
                except Exception:
                    pass

    def on_stage_v_prompt1_default_selected(self, selected_name: str):
        """When default Step 1 prompt combobox changes, fill the Step 1 prompt textbox."""
        prompt_text = self.prompt_manager.get_prompt(selected_name)
        if prompt_text and hasattr(self, 'stage_v_prompt1_text'):
            try:
                self.stage_v_prompt1_text.configure(state="normal")
            except Exception:
                pass
            self.stage_v_prompt1_text.delete("1.0", tk.END)
            self.stage_v_prompt1_text.insert("1.0", prompt_text)
            if hasattr(self, 'stage_v_prompt1_type_var') and self.stage_v_prompt1_type_var.get() == "default":
                try:
                    self.stage_v_prompt1_text.configure(state="disabled")
                except Exception:
                    pass

    def on_stage_v_prompt2_default_selected(self, selected_name: str):
        """When default Step 2 prompt combobox changes, fill the Step 2 prompt textbox."""
        prompt_text = self.prompt_manager.get_prompt(selected_name)
        if prompt_text and hasattr(self, 'stage_v_prompt2_text'):
            try:
                self.stage_v_prompt2_text.configure(state="normal")
            except Exception:
                pass
            self.stage_v_prompt2_text.delete("1.0", tk.END)
            self.stage_v_prompt2_text.insert("1.0", prompt_text)
            if hasattr(self, 'stage_v_prompt2_type_var') and self.stage_v_prompt2_type_var.get() == "default":
                try:
                    self.stage_v_prompt2_text.configure(state="disabled")
                except Exception:
                    pass

    def on_stage_v_prompt3_default_selected(self, selected_name: str):
        """When default Step 3 prompt combobox changes, fill the Step 3 prompt textbox."""
        prompt_text = self.prompt_manager.get_prompt(selected_name)
        if prompt_text and hasattr(self, 'stage_v_prompt3_text'):
            try:
                self.stage_v_prompt3_text.configure(state="normal")
            except Exception:
                pass
            self.stage_v_prompt3_text.delete("1.0", tk.END)
            self.stage_v_prompt3_text.insert("1.0", prompt_text)
            if hasattr(self, 'stage_v_prompt3_type_var') and self.stage_v_prompt3_type_var.get() == "default":
                try:
                    self.stage_v_prompt3_text.configure(state="disabled")
                except Exception:
                    pass
    def on_second_stage_prompt_type_change(self):
        """Switch between default and custom prompt for Stage 2."""
        # Safety: if widgets/vars not created yet, do nothing
        if not hasattr(self, 'second_stage_prompt_type_var') \
           or not hasattr(self, 'second_stage_default_prompt_combo') \
           or not hasattr(self, 'second_stage_prompt_text'):
            return
        mode = self.second_stage_prompt_type_var.get()
        if mode == "default":
            # Enable combobox, show read-only textbox with default prompt
            self.second_stage_default_prompt_combo.configure(state="normal")
            selected_name = self.second_stage_default_prompt_var.get()
            self.on_second_stage_default_prompt_selected(selected_name)
            self.second_stage_prompt_text.configure(state="disabled")
        else:
            # Disable combobox, enable free editing
            self.second_stage_default_prompt_combo.configure(state="disabled")
            self.second_stage_prompt_text.configure(state="normal")

    def on_auto_stage3_prompt_type_change(self):
        """Switch between default and custom prompt for Stage 3."""
        if not hasattr(self, 'auto_stage3_prompt_type_var') \
           or not hasattr(self, 'auto_stage3_default_prompt_combo') \
           or not hasattr(self, 'auto_stage3_prompt_text'):
            return
        mode = self.auto_stage3_prompt_type_var.get()
        if mode == "default":
            self.auto_stage3_default_prompt_combo.configure(state="normal")
            selected_name = self.auto_stage3_default_prompt_var.get()
            self.on_auto_stage3_default_prompt_selected(selected_name)
            self.auto_stage3_prompt_text.configure(state="disabled")
        else:
            self.auto_stage3_default_prompt_combo.configure(state="disabled")
            self.auto_stage3_prompt_text.configure(state="normal")

    def on_auto_stage4_prompt_type_change(self):
        """Switch between default and custom prompt for Stage 4."""
        if not hasattr(self, 'auto_stage4_prompt_type_var') \
           or not hasattr(self, 'auto_stage4_default_prompt_combo') \
           or not hasattr(self, 'auto_stage4_prompt_text'):
            return
        mode = self.auto_stage4_prompt_type_var.get()
        if mode == "default":
            self.auto_stage4_default_prompt_combo.configure(state="normal")
            selected_name = self.auto_stage4_default_prompt_var.get()
            self.on_auto_stage4_default_prompt_selected(selected_name)
            self.auto_stage4_prompt_text.configure(state="disabled")
        else:
            self.auto_stage4_default_prompt_combo.configure(state="disabled")
            self.auto_stage4_prompt_text.configure(state="normal")

    def on_stage_e_prompt_type_change(self):
        """Switch between default and custom prompt for Stage E."""
        if not hasattr(self, 'stage_e_prompt_type_var') \
           or not hasattr(self, 'stage_e_default_prompt_combo') \
           or not hasattr(self, 'stage_e_prompt_text'):
            return
        mode = self.stage_e_prompt_type_var.get()
        if mode == "default":
            self.stage_e_default_prompt_combo.configure(state="normal")
            selected_name = self.stage_e_default_prompt_var.get()
            self.on_stage_e_default_prompt_selected(selected_name)
            try:
                self.stage_e_prompt_text.configure(state="disabled")
            except Exception:
                pass
        else:
            self.stage_e_default_prompt_combo.configure(state="disabled")
            try:
                self.stage_e_prompt_text.configure(state="normal")
            except Exception:
                pass
    
    def on_ocr_extraction_prompt_type_change(self):
        """Switch between default and custom prompt for OCR Extraction."""
        if not hasattr(self, 'ocr_extraction_prompt_type_var') \
           or not hasattr(self, 'ocr_extraction_default_prompt_combo') \
           or not hasattr(self, 'ocr_extraction_prompt_text'):
            return
        mode = self.ocr_extraction_prompt_type_var.get()
        if mode == "default":
            self.ocr_extraction_default_prompt_combo.configure(state="normal")
            selected_name = self.ocr_extraction_default_prompt_var.get()
            self.on_ocr_extraction_default_prompt_selected(selected_name)
            try:
                self.ocr_extraction_prompt_text.configure(state="disabled")
            except Exception:
                pass
        else:
            self.ocr_extraction_default_prompt_combo.configure(state="disabled")
            try:
                self.ocr_extraction_prompt_text.configure(state="normal")
            except Exception:
                pass
    
    def on_ocr_extraction_default_prompt_selected(self, selected_name: str):
        """When default OCR Extraction prompt combobox changes, fill the prompt textbox."""
        prompt_text = self.prompt_manager.get_prompt(selected_name)
        if prompt_text and hasattr(self, 'ocr_extraction_prompt_text'):
            try:
                self.ocr_extraction_prompt_text.configure(state="normal")
            except Exception:
                pass
            self.ocr_extraction_prompt_text.delete("1.0", tk.END)
            self.ocr_extraction_prompt_text.insert("1.0", prompt_text)
            if hasattr(self, 'ocr_extraction_prompt_type_var') and self.ocr_extraction_prompt_type_var.get() == "default":
                try:
                    self.ocr_extraction_prompt_text.configure(state="disabled")
                except Exception:
                    pass
    
    def on_stage_j_prompt_type_change(self):
        """Switch between default and custom prompt for Stage J (Importance & Type)."""
        if not hasattr(self, 'stage_j_prompt_type_var') \
           or not hasattr(self, 'stage_j_default_prompt_combo') \
           or not hasattr(self, 'stage_j_prompt_text'):
            return
        mode = self.stage_j_prompt_type_var.get()
        if mode == "default":
            # Enable combobox, show read-only textbox with default prompt
            self.stage_j_default_prompt_combo.configure(state="normal")
            selected_name = self.stage_j_default_prompt_var.get()
            self.on_stage_j_default_prompt_selected(selected_name)
            try:
                self.stage_j_prompt_text.configure(state="disabled")
            except Exception:
                pass
        else:
            # Disable combobox, enable free editing
            self.stage_j_default_prompt_combo.configure(state="disabled")
            try:
                self.stage_j_prompt_text.configure(state="normal")
            except Exception:
                pass

    def on_stage_l_default_prompt_selected(self, selected_name: str):
        """When default Stage L prompt combobox changes, fill the Stage L prompt textbox."""
        prompt_text = self.prompt_manager.get_prompt(selected_name)
        if prompt_text and hasattr(self, 'stage_l_prompt_text'):
            try:
                self.stage_l_prompt_text.configure(state="normal")
            except Exception:
                pass
            self.stage_l_prompt_text.delete("1.0", tk.END)
            self.stage_l_prompt_text.insert("1.0", prompt_text)
            if hasattr(self, 'stage_l_prompt_type_var') and self.stage_l_prompt_type_var.get() == "default":
                try:
                    self.stage_l_prompt_text.configure(state="disabled")
                except Exception:
                    pass

    def on_stage_l_prompt_type_change(self):
        """Switch between default and custom prompt for Stage L."""
        if not hasattr(self, 'stage_l_prompt_type_var') \
           or not hasattr(self, 'stage_l_default_prompt_combo') \
           or not hasattr(self, 'stage_l_prompt_text'):
            return
        mode = self.stage_l_prompt_type_var.get()
        if mode == "default":
            self.stage_l_default_prompt_combo.configure(state="normal")
            selected_name = self.stage_l_default_prompt_var.get()
            self.on_stage_l_default_prompt_selected(selected_name)
            try:
                self.stage_l_prompt_text.configure(state="disabled")
            except Exception:
                pass
        else:
            self.stage_l_default_prompt_combo.configure(state="disabled")
            try:
                self.stage_l_prompt_text.configure(state="normal")
            except Exception:
                pass
    
    # Stage X PDF Extraction Prompt handlers
    def on_stage_x_pdf_default_prompt_selected(self, selected_name: str):
        """Load default PDF extraction prompt when selected from combobox."""
        prompt_text = self.prompt_manager.get_prompt(selected_name)
        if prompt_text and hasattr(self, 'stage_x_pdf_prompt_text'):
            try:
                self.stage_x_pdf_prompt_text.configure(state="normal")
            except Exception:
                pass
            self.stage_x_pdf_prompt_text.delete("1.0", tk.END)
            self.stage_x_pdf_prompt_text.insert("1.0", prompt_text)
            if hasattr(self, 'stage_x_pdf_prompt_type_var') and self.stage_x_pdf_prompt_type_var.get() == "default":
                try:
                    self.stage_x_pdf_prompt_text.configure(state="disabled")
                except Exception:
                    pass
    
    def on_stage_x_pdf_prompt_type_change(self):
        """Switch between default and custom prompt for Stage X PDF Extraction."""
        if not hasattr(self, 'stage_x_pdf_prompt_type_var') \
           or not hasattr(self, 'stage_x_pdf_default_prompt_combo') \
           or not hasattr(self, 'stage_x_pdf_prompt_text'):
            return
        mode = self.stage_x_pdf_prompt_type_var.get()
        if mode == "default":
            self.stage_x_pdf_default_prompt_combo.configure(state="normal")
            selected_name = self.stage_x_pdf_default_prompt_var.get()
            self.on_stage_x_pdf_default_prompt_selected(selected_name)
            try:
                self.stage_x_pdf_prompt_text.configure(state="disabled")
            except Exception:
                pass
        else:
            self.stage_x_pdf_default_prompt_combo.configure(state="disabled")
            try:
                self.stage_x_pdf_prompt_text.configure(state="normal")
            except Exception:
                pass
    
    # Stage X Change Detection Prompt handlers
    def on_stage_x_change_default_prompt_selected(self, selected_name: str):
        """Load default change detection prompt when selected from combobox."""
        prompt_text = self.prompt_manager.get_prompt(selected_name)
        if prompt_text and hasattr(self, 'stage_x_change_prompt_text'):
            try:
                self.stage_x_change_prompt_text.configure(state="normal")
            except Exception:
                pass
            self.stage_x_change_prompt_text.delete("1.0", tk.END)
            self.stage_x_change_prompt_text.insert("1.0", prompt_text)
            if hasattr(self, 'stage_x_change_prompt_type_var') and self.stage_x_change_prompt_type_var.get() == "default":
                try:
                    self.stage_x_change_prompt_text.configure(state="disabled")
                except Exception:
                    pass
    
    def on_stage_x_change_prompt_type_change(self):
        """Switch between default and custom prompt for Stage X Change Detection."""
        if not hasattr(self, 'stage_x_change_prompt_type_var') \
           or not hasattr(self, 'stage_x_change_default_prompt_combo') \
           or not hasattr(self, 'stage_x_change_prompt_text'):
            return
        mode = self.stage_x_change_prompt_type_var.get()
        if mode == "default":
            self.stage_x_change_default_prompt_combo.configure(state="normal")
            selected_name = self.stage_x_change_default_prompt_var.get()
            self.on_stage_x_change_default_prompt_selected(selected_name)
            try:
                self.stage_x_change_prompt_text.configure(state="disabled")
            except Exception:
                pass
        else:
            self.stage_x_change_default_prompt_combo.configure(state="disabled")
            try:
                self.stage_x_change_prompt_text.configure(state="normal")
            except Exception:
                pass
    
    # Stage Y Prompt handlers
    def on_stage_y_default_prompt_selected(self, selected_name: str):
        """Load default prompt when selected from combobox."""
        prompt_text = self.prompt_manager.get_prompt(selected_name)
        if prompt_text and hasattr(self, 'stage_y_prompt_text'):
            try:
                self.stage_y_prompt_text.configure(state="normal")
            except Exception:
                pass
            self.stage_y_prompt_text.delete("1.0", tk.END)
            self.stage_y_prompt_text.insert("1.0", prompt_text)
            if hasattr(self, 'stage_y_prompt_type_var') and self.stage_y_prompt_type_var.get() == "default":
                try:
                    self.stage_y_prompt_text.configure(state="disabled")
                except Exception:
                    pass
    
    def on_stage_y_prompt_type_change(self):
        """Switch between default and custom prompt for Stage Y."""
        if not hasattr(self, 'stage_y_prompt_type_var') \
           or not hasattr(self, 'stage_y_default_prompt_combo') \
           or not hasattr(self, 'stage_y_prompt_text'):
            return
        mode = self.stage_y_prompt_type_var.get()
        if mode == "default":
            self.stage_y_default_prompt_combo.configure(state="normal")
            selected_name = self.stage_y_default_prompt_var.get()
            self.on_stage_y_default_prompt_selected(selected_name)
            try:
                self.stage_y_prompt_text.configure(state="disabled")
            except Exception:
                pass
        else:
            self.stage_y_default_prompt_combo.configure(state="disabled")
            try:
                self.stage_y_prompt_text.configure(state="normal")
            except Exception:
                pass
    
    def on_stage_y_deletion_default_prompt_selected(self, selected_name: str):
        """When default Stage Y deletion prompt combobox changes, fill the Stage Y deletion prompt textbox."""
        prompt_text = self.prompt_manager.get_prompt(selected_name)
        if prompt_text and hasattr(self, 'stage_y_deletion_prompt_text'):
            try:
                self.stage_y_deletion_prompt_text.configure(state="normal")
            except Exception:
                pass
            self.stage_y_deletion_prompt_text.delete("1.0", tk.END)
            self.stage_y_deletion_prompt_text.insert("1.0", prompt_text)
            if hasattr(self, 'stage_y_deletion_prompt_type_var') and self.stage_y_deletion_prompt_type_var.get() == "default":
                try:
                    self.stage_y_deletion_prompt_text.configure(state="disabled")
                except Exception:
                    pass

    def on_stage_y_deletion_prompt_type_change(self):
        """Switch between default and custom prompt for Stage Y deletion detection."""
        if not hasattr(self, 'stage_y_deletion_prompt_type_var') \
           or not hasattr(self, 'stage_y_deletion_default_prompt_combo') \
           or not hasattr(self, 'stage_y_deletion_prompt_text'):
            return
        mode = self.stage_y_deletion_prompt_type_var.get()
        if mode == "default":
            self.stage_y_deletion_default_prompt_combo.configure(state="normal")
            selected_name = self.stage_y_deletion_default_prompt_var.get()
            self.on_stage_y_deletion_default_prompt_selected(selected_name)
            try:
                self.stage_y_deletion_prompt_text.configure(state="disabled")
            except Exception:
                pass
        else:
            self.stage_y_deletion_default_prompt_combo.configure(state="disabled")
            try:
                self.stage_y_deletion_prompt_text.configure(state="normal")
            except Exception:
                pass
    
    # Stage Z Prompt handlers
    def on_stage_z_default_prompt_selected(self, selected_name: str):
        """Load default prompt when selected from combobox."""
        prompt_text = self.prompt_manager.get_prompt(selected_name)
        if prompt_text and hasattr(self, 'stage_z_prompt_text'):
            try:
                self.stage_z_prompt_text.configure(state="normal")
            except Exception:
                pass
            self.stage_z_prompt_text.delete("1.0", tk.END)
            self.stage_z_prompt_text.insert("1.0", prompt_text)
            if hasattr(self, 'stage_z_prompt_type_var') and self.stage_z_prompt_type_var.get() == "default":
                try:
                    self.stage_z_prompt_text.configure(state="disabled")
                except Exception:
                    pass
    
    def on_stage_z_prompt_type_change(self):
        """Switch between default and custom prompt for Stage Z."""
        if not hasattr(self, 'stage_z_prompt_type_var') \
           or not hasattr(self, 'stage_z_default_prompt_combo') \
           or not hasattr(self, 'stage_z_prompt_text'):
            return
        mode = self.stage_z_prompt_type_var.get()
        if mode == "default":
            self.stage_z_default_prompt_combo.configure(state="normal")
            selected_name = self.stage_z_default_prompt_var.get()
            self.on_stage_z_default_prompt_selected(selected_name)
            try:
                self.stage_z_prompt_text.configure(state="disabled")
            except Exception:
                pass
        else:
            self.stage_z_default_prompt_combo.configure(state="disabled")
            try:
                self.stage_z_prompt_text.configure(state="normal")
            except Exception:
                pass

    def on_stage_h_prompt_type_change(self):
        """Switch between default and custom prompt for Stage H (Flashcard Generation)."""
        if not hasattr(self, 'stage_h_prompt_type_var') \
           or not hasattr(self, 'stage_h_default_prompt_combo') \
           or not hasattr(self, 'stage_h_prompt_text'):
            return
        mode = self.stage_h_prompt_type_var.get()
        if mode == "default":
            # Enable combobox, show read-only textbox with default prompt
            self.stage_h_default_prompt_combo.configure(state="normal")
            selected_name = self.stage_h_default_prompt_var.get()
            self.on_stage_h_default_prompt_selected(selected_name)
            try:
                self.stage_h_prompt_text.configure(state="disabled")
            except Exception:
                pass

    def on_stage_v_prompt1_type_change(self):
        """Switch between default and custom prompt for Stage V Step 1."""
        if not hasattr(self, 'stage_v_prompt1_type_var') \
           or not hasattr(self, 'stage_v_prompt1_default_combo') \
           or not hasattr(self, 'stage_v_prompt1_text'):
            return
        mode = self.stage_v_prompt1_type_var.get()
        if mode == "default":
            self.stage_v_prompt1_default_combo.configure(state="normal")
            selected_name = self.stage_v_prompt1_default_var.get()
            self.on_stage_v_prompt1_default_selected(selected_name)
            try:
                self.stage_v_prompt1_text.configure(state="disabled")
            except Exception:
                pass
        else:
            self.stage_v_prompt1_default_combo.configure(state="disabled")
            try:
                self.stage_v_prompt1_text.configure(state="normal")
            except Exception:
                pass

    def on_stage_v_prompt2_type_change(self):
        """Switch between default and custom prompt for Stage V Step 2."""
        if not hasattr(self, 'stage_v_prompt2_type_var') \
           or not hasattr(self, 'stage_v_prompt2_default_combo') \
           or not hasattr(self, 'stage_v_prompt2_text'):
            return
        mode = self.stage_v_prompt2_type_var.get()
        if mode == "default":
            self.stage_v_prompt2_default_combo.configure(state="normal")
            selected_name = self.stage_v_prompt2_default_var.get()
            self.on_stage_v_prompt2_default_selected(selected_name)
            try:
                self.stage_v_prompt2_text.configure(state="disabled")
            except Exception:
                pass
        else:
            self.stage_v_prompt2_default_combo.configure(state="disabled")
            try:
                self.stage_v_prompt2_text.configure(state="normal")
            except Exception:
                pass

    def on_stage_v_prompt3_type_change(self):
        """Switch between default and custom prompt for Stage V Step 3."""
        if not hasattr(self, 'stage_v_prompt3_type_var') \
           or not hasattr(self, 'stage_v_prompt3_default_combo') \
           or not hasattr(self, 'stage_v_prompt3_text'):
            return
        mode = self.stage_v_prompt3_type_var.get()
        if mode == "default":
            self.stage_v_prompt3_default_combo.configure(state="normal")
            selected_name = self.stage_v_prompt3_default_var.get()
            self.on_stage_v_prompt3_default_selected(selected_name)
            try:
                self.stage_v_prompt3_text.configure(state="disabled")
            except Exception:
                pass
        else:
            self.stage_v_prompt3_default_combo.configure(state="disabled")
            try:
                self.stage_v_prompt3_text.configure(state="normal")
            except Exception:
                pass

    def process_multiple_pre_ocr_topics(self):
        """Process multiple PDF files for Pre-OCR Topic Extraction"""
        def worker():
            try:
                if not self.pre_ocr_selected_files:
                    self.root.after(0, lambda: messagebox.showwarning("Warning", "Please select at least one PDF file"))
                    return
                
                # Disable button
                self.root.after(0, lambda: self.pre_ocr_process_all_btn.configure(state="disabled", text="Processing..."))
                
                # Get prompt
                prompt = self.pre_ocr_prompt_text.get("1.0", tk.END).strip()
                if not prompt:
                    default_prompt = self.prompt_manager.get_prompt("Pre OCR Topic")
                    if default_prompt:
                        prompt = default_prompt
                        self.logger.info("Using default Pre OCR Topic prompt from prompts.json")
                    else:
                        self.root.after(0, lambda: messagebox.showerror("Error", "Please enter a prompt"))
                        return
                
                # Get delay
                try:
                    delay_seconds = int(self.pre_ocr_delay_var.get())
                except ValueError:
                    delay_seconds = 5
                    self.logger.warning(f"Invalid delay value, using default: {delay_seconds} seconds")
                
                # Validate API keys
                if not self.api_key_manager.api_keys:
                    if not self.api_key_file_var.get():
                        self.root.after(0, lambda: messagebox.showerror("Error", "Please load API keys"))
                        return
                    else:
                        if not self.api_key_manager.load_from_csv(self.api_key_file_var.get()):
                            self.root.after(0, lambda: messagebox.showerror("Error", "Failed to load API keys"))
                            return
                
                model_name = self.pre_ocr_model_var.get()
                total_files = len(self.pre_ocr_selected_files)
                completed = 0
                failed = 0
                
                # Reset progress bar
                self.root.after(0, lambda: self.pre_ocr_progress_bar.set(0))
                
                # Process each file
                for idx, file_info in enumerate(self.pre_ocr_selected_files):
                    file_path = file_info['path']
                    status_label = file_info['status_label']
                    file_name = os.path.basename(file_path)
                    
                    # Update status to processing
                    self.root.after(0, lambda sl=status_label, fn=file_name: 
                                   sl.configure(text="Processing...", text_color="blue"))
                    
                    self.root.after(0, lambda idx=idx, total=total_files, fp=file_path: 
                                   self.pre_ocr_progress_label.configure(
                                       text=f"Processing file {idx+1}/{total}: {os.path.basename(fp)}"))
                    
                    # Progress bar
                    progress = idx / total_files
                    self.root.after(0, lambda p=progress: self.pre_ocr_progress_bar.set(p))
                    
                    try:
                        # Progress callback for this file
                        def progress_callback(msg: str):
                            self.root.after(0, lambda m=msg: self.pre_ocr_status_label.configure(text=m))
                        
                        # Process the file
                        output_path = self.pre_ocr_topic_processor.process_pre_ocr_topic(
                            pdf_path=file_path,
                            prompt=prompt,
                            model_name=model_name,
                            output_dir=self.get_default_output_dir(file_path),
                            progress_callback=progress_callback
                        )
                        
                        if output_path and os.path.exists(output_path):
                            file_info['output_path'] = output_path
                            completed += 1
                            self.root.after(0, lambda sl=status_label, fn=file_name, op=output_path: 
                                          sl.configure(text="Completed", text_color="green"))
                            self.logger.info(f"Completed: {file_name} -> {output_path}")
                        else:
                            failed += 1
                            self.root.after(0, lambda sl=status_label, fn=file_name: 
                                          sl.configure(text="Failed", text_color="red"))
                            self.logger.error(f"Failed: {file_name}")
                    
                    except Exception as e:
                        failed += 1
                        self.root.after(0, lambda sl=status_label, fn=file_name: 
                                       sl.configure(text="Error", text_color="red"))
                        self.logger.error(f"Error processing {file_name}: {e}", exc_info=True)
                    
                    # Delay before next file (except for the last one)
                    if idx < total_files - 1:
                        self.root.after(0, lambda d=delay_seconds: 
                                       self.pre_ocr_progress_label.configure(
                                           text=f"Waiting {d} seconds before next file..."))
                        time.sleep(delay_seconds)
                
                # Final update
                self.root.after(0, lambda: self.pre_ocr_progress_bar.set(1.0))
                self.root.after(0, lambda c=completed, f=failed: 
                               self.pre_ocr_progress_label.configure(
                                   text=f"Batch processing completed! {c} succeeded, {f} failed"))
                
                # Show summary
                summary = f"Batch Processing Complete!\n\n" \
                         f"Total: {total_files}\n" \
                         f"Completed: {completed}\n" \
                         f"Failed: {failed}"
                self.root.after(0, lambda s=summary: messagebox.showinfo("Batch Processing Complete", s))
                
            except Exception as e:
                error_msg = f"Batch processing error: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                self.root.after(0, lambda msg=error_msg: messagebox.showerror("Error", msg))
            finally:
                # Re-enable button
                self.root.after(0, lambda: self.pre_ocr_process_all_btn.configure(state="normal", text="Process All Files"))
        
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()

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
                
                # Get selected model (use default from main view)
                model_name = self.get_default_model()
                self.logger.info(f"[process_pdf] Using model: {model_name}")
                
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
                
                # Use multi-part processor (use default output dir from main view)
                final_output_path = self.multi_part_processor.process_multi_part(
                    pdf_path=self.pdf_path,
                    base_prompt=prompt,  # User's original prompt (no modifications)
                    model_name=model_name,
                    temperature=0.7,
                    resume=True,  # Enable resume capability
                    progress_callback=progress_callback,
                    output_dir=self.get_default_output_dir(self.pdf_path)
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
                    self.update_status("Multi-part processing failed or was incomplete")
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
    
    def convert_json_to_csv(self, json_file_path: str) -> Optional[str]:
        """
        Convert JSON file to CSV format with ";;;" delimiter.
        Supports multiple JSON structures: {rows: [...]}, {data: [...]}, {points: [...]}
        
        Args:
            json_file_path: Path to JSON file
            
        Returns:
            CSV text string or None on error
        """
        try:
            # Load JSON file
            with open(json_file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            # Extract rows from JSON (support multiple structures)
            rows = []
            if 'rows' in json_data:
                rows = json_data.get('rows', [])
            elif 'data' in json_data:
                rows = json_data.get('data', [])
            elif 'points' in json_data:
                rows = json_data.get('points', [])
            elif isinstance(json_data, list):
                rows = json_data
            else:
                messagebox.showwarning("No Data", "The JSON file contains no recognizable data structure.")
                return None
            
            if not rows:
                messagebox.showwarning("No Data", "The JSON file contains no data rows.")
                return None
            
            # Convert to CSV format with ";;;" delimiter
            delimiter = ";;;"
            
            # Get headers from first row
            if not isinstance(rows, list) or len(rows) == 0:
                messagebox.showerror("Error", "Invalid JSON format: no rows found")
                return None
            
            headers = list(rows[0].keys())
            
            # Build CSV with header first
            csv_lines = [delimiter.join(headers)]
            
            # Add data rows
            for row in rows:
                csv_line = delimiter.join(str(row.get(h, "")) for h in headers)
                csv_lines.append(csv_line)
            
            csv_text = "\n".join(csv_lines)
            return csv_text
            
        except json.JSONDecodeError as e:
            messagebox.showerror("Error", f"Failed to parse JSON file:\n{str(e)}")
            self.logger.error(f"Failed to parse JSON: {str(e)}")
            return None
        except Exception as e:
            messagebox.showerror("Error", f"Error converting JSON to CSV:\n{str(e)}")
            self.logger.error(f"Error converting JSON to CSV: {str(e)}", exc_info=True)
            return None
    
    def view_csv_from_json(self):
        """Convert the last processed JSON file (Document Processing) to CSV format and display it"""
        if not self.last_final_output_path or not os.path.exists(self.last_final_output_path):
            messagebox.showerror("Error", "No JSON file available. Please process a PDF first.")
            return
        
        csv_text = self.convert_json_to_csv(self.last_final_output_path)
        if csv_text:
            csv_file_path = self.last_final_output_path.replace('.json', '.csv')
            self.show_response_window(csv_text, csv_file_path, is_csv=True, is_json=False)
    
    def view_csv_stage_e(self):
        """Convert Image Notes JSON to CSV format and display it"""
        if not self.last_stage_e_path or not os.path.exists(self.last_stage_e_path):
            messagebox.showerror("Error", "No Image Notes JSON file available. Please process Image Notes Generation first.")
            return
        
        csv_text = self.convert_json_to_csv(self.last_stage_e_path)
        if csv_text:
            csv_file_path = self.last_stage_e_path.replace('.json', '.csv')
            self.show_response_window(csv_text, csv_file_path, is_csv=True, is_json=False)
    
    def view_csv_stage_f(self):
        """Convert Image File Catalog JSON to CSV format and display it"""
        if not self.last_stage_f_path or not os.path.exists(self.last_stage_f_path):
            messagebox.showerror("Error", "No Image File Catalog JSON file available. Please process Image File Catalog first.")
            return
        
        csv_text = self.convert_json_to_csv(self.last_stage_f_path)
        if csv_text:
            csv_file_path = self.last_stage_f_path.replace('.json', '.csv')
            self.show_response_window(csv_text, csv_file_path, is_csv=True, is_json=False)
    
    def view_csv_stage_j(self):
        """Convert Tagged Data JSON to CSV format and display it"""
        if not self.last_stage_j_path or not os.path.exists(self.last_stage_j_path):
            messagebox.showerror("Error", "No Tagged Data JSON file available. Please process Importance & Type Tagging first.")
            return
        
        csv_text = self.convert_json_to_csv(self.last_stage_j_path)
        if csv_text:
            csv_file_path = self.last_stage_j_path.replace('.json', '.csv')
            self.show_response_window(csv_text, csv_file_path, is_csv=True, is_json=False)
    
    def view_csv_stage_h(self):
        """Convert Flashcard JSON to CSV format and display it"""
        if not self.last_stage_h_path or not os.path.exists(self.last_stage_h_path):
            messagebox.showerror("Error", "No Flashcard JSON file available. Please process Flashcard Generation first.")
            return
        
        csv_text = self.convert_json_to_csv(self.last_stage_h_path)
        if csv_text:
            csv_file_path = self.last_stage_h_path.replace('.json', '.csv')
            self.show_response_window(csv_text, csv_file_path, is_csv=True, is_json=False)
    
    def view_csv_stage_v(self):
        """Convert Test Bank JSON to CSV format and display it"""
        if not self.last_stage_v_path or not os.path.exists(self.last_stage_v_path):
            messagebox.showerror("Error", "No Test Bank JSON file available. Please process Test Bank Generation first.")
            return
        
        csv_text = self.convert_json_to_csv(self.last_stage_v_path)
        if csv_text:
            csv_file_path = self.last_stage_v_path.replace('.json', '.csv')
            self.show_response_window(csv_text, csv_file_path, is_csv=True, is_json=False)
    
    def view_csv_stage_m(self):
        """Convert Topic List JSON to CSV format and display it"""
        if not self.last_stage_m_path or not os.path.exists(self.last_stage_m_path):
            messagebox.showerror("Error", "No Topic List JSON file available. Please process Topic List Extraction first.")
            return
        
        csv_text = self.convert_json_to_csv(self.last_stage_m_path)
        if csv_text:
            csv_file_path = self.last_stage_m_path.replace('.json', '.csv')
            self.show_response_window(csv_text, csv_file_path, is_csv=True, is_json=False)
    
    def view_csv_stage_l(self):
        """Convert Chapter Summary JSON to CSV format and display it"""
        if not self.last_stage_l_path or not os.path.exists(self.last_stage_l_path):
            messagebox.showerror("Error", "No Chapter Summary JSON file available. Please process Chapter Summary first.")
            return
        
        csv_text = self.convert_json_to_csv(self.last_stage_l_path)
        if csv_text:
            csv_file_path = self.last_stage_l_path.replace('.json', '.csv')
            self.show_response_window(csv_text, csv_file_path, is_csv=True, is_json=False)
    
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

        # Stage 2 prompt mode (default vs custom)
        stage2_mode_frame = ctk.CTkFrame(stage2_frame)
        stage2_mode_frame.pack(fill="x", padx=10, pady=(0, 5))
        if not hasattr(self, 'second_stage_prompt_type_var'):
            self.second_stage_prompt_type_var = ctk.StringVar(value="default")
        ctk.CTkRadioButton(
            stage2_mode_frame,
            text="Use Default Prompt",
            variable=self.second_stage_prompt_type_var,
            value="default",
            command=self.on_second_stage_prompt_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)
        ctk.CTkRadioButton(
            stage2_mode_frame,
            text="Use Custom Prompt",
            variable=self.second_stage_prompt_type_var,
            value="custom",
            command=self.on_second_stage_prompt_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)

        # Default Stage 2 prompt combobox (predefined prompts)
        stage2_default_frame = ctk.CTkFrame(stage2_frame)
        stage2_default_frame.pack(fill="x", padx=10, pady=(0, 10))

        ctk.CTkLabel(
            stage2_default_frame,
            text="Default Stage 2 Prompt:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", pady=(0, 5))

        stage2_prompt_names = self.prompt_manager.get_prompt_names()
        # Prefer the dedicated Stage 2 prompt if available
        preferred_stage2_name = "Document Processing Prompt"
        if preferred_stage2_name in stage2_prompt_names:
            stage2_default_value = preferred_stage2_name
        else:
            stage2_default_value = stage2_prompt_names[0] if stage2_prompt_names else ""
        self.second_stage_default_prompt_var = ctk.StringVar(value=stage2_default_value)
        self.second_stage_default_prompt_combo = ctk.CTkComboBox(
            stage2_default_frame,
            values=stage2_prompt_names,
            variable=self.second_stage_default_prompt_var,
            width=400,
            command=self.on_second_stage_default_prompt_selected,
        )
        self.second_stage_default_prompt_combo.pack(anchor="w", pady=(0, 5))

        # Textbox for Stage 2 prompt (default-filled or custom)
        self.second_stage_prompt_text = ctk.CTkTextbox(stage2_frame, height=140, font=self.farsi_text_font)
        self.second_stage_prompt_text.pack(fill="x", padx=10, pady=(0, 10))
        # پر کردن خودکار با پرامپت پیش‌فرض در شروع
        # First ensure the prompt type change handler runs
        self.on_second_stage_prompt_type_change()
        
        # Also directly load the default prompt to ensure it's displayed
        if hasattr(self, 'second_stage_default_prompt_var'):
            default_prompt_name = self.second_stage_default_prompt_var.get()
            if default_prompt_name:
                self.on_second_stage_default_prompt_selected(default_prompt_name)
        
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
            text="Document Processing Progress",
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

        # Stage 2 prompt mode (default vs custom) - tabview version (shares state with popup window)
        stage2_mode_frame_tab = ctk.CTkFrame(stage2_frame)
        stage2_mode_frame_tab.pack(fill="x", padx=10, pady=(0, 5))
        if not hasattr(self, 'second_stage_prompt_type_var'):
            self.second_stage_prompt_type_var = ctk.StringVar(value="default")
        ctk.CTkRadioButton(
            stage2_mode_frame_tab,
            text="Use Default Prompt",
            variable=self.second_stage_prompt_type_var,
            value="default",
            command=self.on_second_stage_prompt_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)
        ctk.CTkRadioButton(
            stage2_mode_frame_tab,
            text="Use Custom Prompt",
            variable=self.second_stage_prompt_type_var,
            value="custom",
            command=self.on_second_stage_prompt_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)

        # Default Stage 2 prompt combobox (predefined prompts) for tabview
        stage2_default_frame_tab = ctk.CTkFrame(stage2_frame)
        stage2_default_frame_tab.pack(fill="x", padx=10, pady=(0, 10))

        ctk.CTkLabel(
            stage2_default_frame_tab,
            text="Default Document Processing Prompt:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", pady=(0, 5))

        stage2_prompt_names_tab = self.prompt_manager.get_prompt_names()
        preferred_stage2_name = "Document Processing Prompt"
        if preferred_stage2_name in stage2_prompt_names_tab:
            stage2_default_value_tab = preferred_stage2_name
        else:
            stage2_default_value_tab = stage2_prompt_names_tab[0] if stage2_prompt_names_tab else ""
        if not hasattr(self, 'second_stage_default_prompt_var'):
            self.second_stage_default_prompt_var = ctk.StringVar(value=stage2_default_value_tab)
        # Use shared combobox name so the prompt-type logic applies in this tab as well
        self.second_stage_default_prompt_combo = ctk.CTkComboBox(
            stage2_default_frame_tab,
            values=stage2_prompt_names_tab,
            variable=self.second_stage_default_prompt_var,
            width=400,
            command=self.on_second_stage_default_prompt_selected,
        )
        self.second_stage_default_prompt_combo.pack(anchor="w", pady=(0, 5))

        # Textbox for Stage 2 prompt (default-filled or custom) for tabview
        if not hasattr(self, 'second_stage_prompt_text'):
            self.second_stage_prompt_text = ctk.CTkTextbox(stage2_frame, height=140, font=self.farsi_text_font)
        else:
            try:
                self.second_stage_prompt_text.pack_forget()
            except Exception:
                pass
        self.second_stage_prompt_text.pack(fill="x", padx=10, pady=(0, 10))
        
        # پر کردن خودکار با پرامپت پیش‌فرض در شروع
        # First ensure the prompt type change handler runs
        self.on_second_stage_prompt_type_change()
        
        # Also directly load the default prompt to ensure it's displayed
        if hasattr(self, 'second_stage_default_prompt_var'):
            default_prompt_name = self.second_stage_default_prompt_var.get()
            if default_prompt_name:
                self.on_second_stage_default_prompt_selected(default_prompt_name)

        # Info label about placeholders
        info_frame = ctk.CTkFrame(stage2_frame)
        info_frame.pack(fill="x", padx=10, pady=(0, 10))
        
        ctk.CTkLabel(
            info_frame,
            text="Note: Your prompt should contain {Topic_NAME} and {Subchapter_Name} placeholders.",
            font=ctk.CTkFont(size=10),
            text_color="gray",
        ).pack(anchor="w", padx=10, pady=(5, 5))
        
        ctk.CTkLabel(
            info_frame,
            text="These will be automatically replaced with the actual topic and subchapter names for each topic.",
            font=ctk.CTkFont(size=10),
            text_color="gray",
        ).pack(anchor="w", padx=10, pady=(0, 10))
        
        # JSON selection section - Batch Processing
        json_frame = ctk.CTkFrame(main_frame)
        json_frame.pack(fill="x", pady=(0, 20))
        
        ctk.CTkLabel(
            json_frame,
            text="Input JSON Files (OCR Extraction JSON) - Batch Processing",
            font=ctk.CTkFont(size=18, weight="bold"),
        ).pack(pady=(15, 10))
        
        # File selection buttons
        buttons_frame = ctk.CTkFrame(json_frame)
        buttons_frame.pack(fill="x", padx=15, pady=5)
        
        def browse_multiple_document_processing_files():
            filenames = filedialog.askopenfilenames(
                title="Select OCR Extraction JSON files",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            if filenames:
                if not hasattr(self, 'document_processing_selected_files'):
                    self.document_processing_selected_files = []
                for filename in filenames:
                    if filename not in self.document_processing_selected_files:
                        self.document_processing_selected_files.append(filename)
                        self._add_document_processing_file_to_ui(filename)
        
        def select_folder_and_match_document_processing_files():
            folder_path = filedialog.askdirectory(title="Select folder containing OCR Extraction JSON files")
            if not folder_path:
                return
            
            import glob
            json_files = glob.glob(os.path.join(folder_path, "*.json"))
            json_files = [f for f in json_files if os.path.isfile(f)]
            
            if not json_files:
                messagebox.showinfo("Info", "No JSON files found in selected folder")
                return
            
            if not hasattr(self, 'document_processing_selected_files'):
                self.document_processing_selected_files = []
            
            added_count = 0
            for json_file in json_files:
                if json_file not in self.document_processing_selected_files:
                    self.document_processing_selected_files.append(json_file)
                    self._add_document_processing_file_to_ui(json_file)
                    added_count += 1
            
            messagebox.showinfo("Success", f"Added {added_count} JSON file(s) from folder")
        
        def add_document_processing_file_manual():
            filename = filedialog.askopenfilename(
                title="Select OCR Extraction JSON file",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            if filename:
                if not hasattr(self, 'document_processing_selected_files'):
                    self.document_processing_selected_files = []
                if filename not in self.document_processing_selected_files:
                    self.document_processing_selected_files.append(filename)
                    self._add_document_processing_file_to_ui(filename)
        
        ctk.CTkButton(
            buttons_frame,
            text="Browse Multiple Files",
            command=browse_multiple_document_processing_files,
            width=180,
        ).pack(side="left", padx=5, pady=5)
        
        ctk.CTkButton(
            buttons_frame,
            text="Select Folder (Auto-Match)",
            command=select_folder_and_match_document_processing_files,
            width=180,
        ).pack(side="left", padx=5, pady=5)
        
        ctk.CTkButton(
            buttons_frame,
            text="Add File Manually",
            command=add_document_processing_file_manual,
            width=150,
        ).pack(side="left", padx=5, pady=5)
        
        # Delay setting
        delay_frame = ctk.CTkFrame(json_frame)
        delay_frame.pack(fill="x", padx=15, pady=5)
        
        ctk.CTkLabel(
            delay_frame,
            text="Delay between files (seconds):",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(side="left", padx=10, pady=5)
        
        if not hasattr(self, 'document_processing_delay_var'):
            self.document_processing_delay_var = ctk.StringVar(value="5")
        
        delay_entry = ctk.CTkEntry(delay_frame, textvariable=self.document_processing_delay_var, width=100)
        delay_entry.pack(side="left", padx=5, pady=5)
        
        # Files list (scrollable)
        list_frame = ctk.CTkFrame(json_frame)
        list_frame.pack(fill="both", expand=True, padx=15, pady=5)
        
        ctk.CTkLabel(
            list_frame,
            text="Selected Files:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'document_processing_files_list_scroll'):
            self.document_processing_files_list_scroll = ctk.CTkScrollableFrame(list_frame, height=150)
        self.document_processing_files_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize selected files list if not exists
        if not hasattr(self, 'document_processing_selected_files'):
            self.document_processing_selected_files = []
        
        ctk.CTkLabel(
            json_frame,
            text="Each JSON file should have the structure: chapters -> subchapters -> topics -> extractions",
            font=ctk.CTkFont(size=10),
            text_color="gray",
        ).pack(anchor="w", padx=15, pady=(0, 10))
        
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
            text="Select model for second-stage (per-Topic) processing:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        # Only create if doesn't exist
        if not hasattr(self, 'second_stage_model_var'):
            self.second_stage_model_var = ctk.StringVar(value=self.model_var.get() if hasattr(self, 'model_var') else "gemini-2.5-pro")
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
            text="Model will be used for Document Processing (per-Topic processing).",
            font=ctk.CTkFont(size=10),
            text_color="gray",
        ).pack(anchor="w", padx=10, pady=(0, 5))

        #
        # --- PointId Settings ---
        #
        pointid_frame = ctk.CTkFrame(main_frame)
        pointid_frame.pack(fill="x", pady=(0, 20))
        
        ctk.CTkLabel(
            pointid_frame,
            text="PointId Configuration",
            font=ctk.CTkFont(size=18, weight="bold"),
        ).pack(pady=(15, 10))
        
        # Option 1: Use PointId mapping file (TXT)
        pointid_option_frame = ctk.CTkFrame(pointid_frame)
        pointid_option_frame.pack(fill="x", padx=15, pady=5)
        
        if not hasattr(self, 'use_pointid_mapping_var'):
            self.use_pointid_mapping_var = ctk.BooleanVar(value=False)
            self.document_processing_pointid_txt_var = ctk.StringVar()
        
        pointid_mapping_checkbox = ctk.CTkCheckBox(
            pointid_option_frame,
            text="Use PointId mapping file (TXT)",
            variable=self.use_pointid_mapping_var,
        )
        pointid_mapping_checkbox.pack(anchor="w", padx=10, pady=5)
        
        pointid_file_frame = ctk.CTkFrame(pointid_option_frame)
        pointid_file_frame.pack(fill="x", padx=20, pady=5)
        
        pointid_entry = ctk.CTkEntry(
            pointid_file_frame,
            textvariable=self.document_processing_pointid_txt_var,
            width=400,
            state="disabled"
        )
        pointid_entry.pack(side="left", fill="x", expand=True, padx=(10, 5))
        
        def load_pointids_from_file(txt_path: str) -> List[str]:
            """Extract PointIds from TXT file"""
            pointids = []
            try:
                with open(txt_path, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        # Skip empty lines and comments
                        if not line or line.startswith('#'):
                            continue
                        
                        # Validate format: must be 10 digits
                        if len(line) == 10 and line.isdigit():
                            pointids.append(line)
            except Exception as e:
                self.logger.error(f"Error loading PointIds from {txt_path}: {e}")
            return pointids
        
        def browse_pointid_txt():
            filename = filedialog.askopenfilename(
                title="Select PointId Mapping TXT file",
                filetypes=[("TXT files", "*.txt"), ("All files", "*.*")]
            )
            if filename:
                self.document_processing_pointid_txt_var.set(filename)
                # Extract and display pointids immediately
                pointids = load_pointids_from_file(filename)
                if pointids:
                    # Update the display textbox
                    pointids_text = "\n".join(pointids)
                    pointids_display.delete("1.0", tk.END)
                    pointids_display.insert("1.0", f"Extracted {len(pointids)} PointIds:\n\n{pointids_text}")
                    pointids_display_frame.pack(fill="both", expand=True, padx=30, pady=(5, 10))
                else:
                    pointids_display.delete("1.0", tk.END)
                    pointids_display.insert("1.0", "No valid PointIds found in the file.")
                    pointids_display_frame.pack(fill="both", expand=True, padx=30, pady=(5, 10))
        
        def toggle_pointid_entry():
            if self.use_pointid_mapping_var.get():
                pointid_entry.configure(state="normal")
                start_pointid_entry.configure(state="disabled")
            else:
                pointid_entry.configure(state="disabled")
                start_pointid_entry.configure(state="normal")
        
        pointid_mapping_checkbox.configure(command=toggle_pointid_entry)
        
        ctk.CTkButton(
            pointid_file_frame,
            text="Browse",
            command=browse_pointid_txt,
            width=80,
        ).pack(side="right", padx=(5, 10))
        
        ctk.CTkLabel(
            pointid_option_frame,
            text="Format: Each line = start PointId for one chapter (10 digits, e.g., 1251330001)",
            font=ctk.CTkFont(size=10),
            text_color="gray",
        ).pack(anchor="w", padx=30, pady=(0, 5))
        
        # Create PointIds display area (will be shown when file is selected)
        pointids_display_frame = ctk.CTkFrame(pointid_option_frame)
        pointids_display_frame.pack_forget()  # Initially hidden
        
        ctk.CTkLabel(
            pointids_display_frame,
            text="Extracted PointIds:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(5, 5))
        
        pointids_display = ctk.CTkTextbox(
            pointids_display_frame,
            height=150,
            font=ctk.CTkFont(size=11),
            wrap="word"
        )
        pointids_display.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Option 2: Manual Start PointId (fallback)
        ctk.CTkLabel(
            pointid_option_frame,
            text="OR use manual Start PointId (if mapping file not used):",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        start_pointid_row = ctk.CTkFrame(pointid_option_frame)
        start_pointid_row.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(
            start_pointid_row,
            text="Start PointId (e.g. 1050030001):",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(5, 5))
        
        # Only create if doesn't exist
        if not hasattr(self, 'auto_start_pointid_var'):
            self.auto_start_pointid_var = ctk.StringVar(value="")
        
        start_pointid_entry = ctk.CTkEntry(
            start_pointid_row,
            textvariable=self.auto_start_pointid_var,
            width=300,
        )
        start_pointid_entry.pack(anchor="w", padx=10, pady=(0, 5))
        
        ctk.CTkLabel(
            start_pointid_row,
            text="If empty, a default starting PointId will be used.",
            font=ctk.CTkFont(size=10),
            text_color="gray",
        ).pack(anchor="w", padx=10, pady=(0, 5))
        
        #
        # --- Progress & control buttons ---
        #
        progress_frame = ctk.CTkFrame(main_frame)
        progress_frame.pack(fill="x", pady=(10, 10))
        
        # Control buttons
        controls_frame = ctk.CTkFrame(main_frame)
        controls_frame.pack(fill="x", pady=(10, 10))
        
        # Full pipeline button
        if not hasattr(self, 'full_pipeline_cancel'):
            self.full_pipeline_cancel = False
        
        def start_full_pipeline():
            full_btn.configure(state="disabled", text="Processing...")
            self.full_pipeline_cancel = False
            threading.Thread(
                target=self.process_multiple_document_processing_files,
                args=(self.root, full_btn),
                daemon=True,
            ).start()
        
        # Progress bar for batch processing
        if not hasattr(self, 'document_processing_progress_bar'):
            self.document_processing_progress_bar = ctk.CTkProgressBar(progress_frame)
        self.document_processing_progress_bar.pack(fill="x", padx=10, pady=(0, 5))
        self.document_processing_progress_bar.set(0.0)
        
        if not hasattr(self, 'document_processing_progress_label'):
            self.document_processing_progress_label = ctk.CTkLabel(
                progress_frame,
                text="Waiting to start...",
                font=ctk.CTkFont(size=10),
                text_color="gray",
            )
        self.document_processing_progress_label.pack(anchor="w", padx=10, pady=(0, 5))
        
        full_btn = ctk.CTkButton(
            controls_frame,
            text="Process All Files",
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

            # Always use default model from main view settings
            model_name = self.get_default_model()

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
            # پایان پردازش Stage 2 – نادیده گرفتن متن دکمه (ممکن است از خارج مدیریت شود)
            try:
                self.root.after(0, lambda: start_button.configure(state="normal"))
            except Exception:
                pass

    def process_multiple_document_processing_files(self, parent_window, start_button):
        """Process multiple OCR Extraction JSON files for Document Processing"""
        try:
            if not hasattr(self, 'document_processing_selected_files') or not self.document_processing_selected_files:
                self.root.after(0, lambda: messagebox.showwarning("Warning", "Please add at least one OCR Extraction JSON file"))
                return
            
            # Get prompt
            document_prompt = self.second_stage_prompt_text.get("1.0", tk.END).strip()
            if not document_prompt:
                default_prompt = self.prompt_manager.get_prompt("Document Processing Prompt")
                if default_prompt:
                    document_prompt = default_prompt
                    self.logger.info("Using default Document Processing prompt from prompts.json")
                else:
                    self.root.after(0, lambda: messagebox.showerror("Error", "Please enter a prompt"))
                    return
            
            # Check if prompt contains placeholders
            if "{Topic_NAME}" not in document_prompt or "{Subchapter_Name}" not in document_prompt:
                self.logger.warning("Document Processing prompt may not contain {Topic_NAME} and {Subchapter_Name} placeholders")
            
            # Get delay
            try:
                delay_seconds = int(self.document_processing_delay_var.get())
            except ValueError:
                delay_seconds = 5
                self.logger.warning(f"Invalid delay value, using default: {delay_seconds} seconds")
            
            # Validate API keys
            if not self.api_key_manager.api_keys:
                self.root.after(0, lambda: messagebox.showerror("Error", "Please load API keys first"))
                return
            
            # PointId handling
            pointid_mapping_txt = None
            if self.use_pointid_mapping_var.get():
                pointid_mapping_txt = self.document_processing_pointid_txt_var.get().strip()
                if pointid_mapping_txt and not os.path.exists(pointid_mapping_txt):
                    self.root.after(0, lambda: messagebox.showerror("Error", f"PointId mapping file not found:\n{pointid_mapping_txt}"))
                    return
            
            # If not using mapping, get manual start PointId
            book_id = None
            chapter_id_num = None
            start_point_index = 1
            if not pointid_mapping_txt:
                start_pointid_str = self.auto_start_pointid_var.get().strip()
                if start_pointid_str:
                    if not (start_pointid_str.isdigit() and len(start_pointid_str) == 10):
                        self.root.after(0, lambda: messagebox.showerror("Error", "Start PointId must be a 10-digit number, e.g. 1050030000."))
                        return
                    try:
                        book_id = int(start_pointid_str[0:3])
                        chapter_id_num = int(start_pointid_str[3:6])
                        start_point_index = int(start_pointid_str[6:10])
                    except ValueError:
                        self.root.after(0, lambda: messagebox.showerror("Error", "Start PointId format is invalid."))
                        return
            
            model_name = self.get_default_model()
            total_files = len(self.document_processing_selected_files)
            completed = 0
            failed = 0
            
            # Reset progress bar
            self.root.after(0, lambda: self.document_processing_progress_bar.set(0))
            
            # Process each file
            for idx, json_file_path in enumerate(self.document_processing_selected_files):
                if self.full_pipeline_cancel:
                    self.root.after(0, lambda: self.document_processing_progress_label.configure(
                        text="Cancelled by user", text_color="orange"))
                    break
                
                file_name = os.path.basename(json_file_path)
                
                # Update status to processing
                if hasattr(self, 'document_processing_file_info_list'):
                    for file_info in self.document_processing_file_info_list:
                        if file_info['file_path'] == json_file_path:
                            self.root.after(0, lambda sl=file_info['status_label']: 
                                           sl.configure(text="Processing...", text_color="blue"))
                            break
                
                self.root.after(0, lambda idx=idx, total=total_files, fn=file_name: 
                               self.document_processing_progress_label.configure(
                                   text=f"Processing file {idx+1}/{total}: {fn}"))
                
                # Progress bar
                progress = idx / total_files
                self.root.after(0, lambda p=progress: self.document_processing_progress_bar.set(p))
                
                try:
                    # Progress callback for this file
                    def progress_callback(msg: str):
                        self.root.after(0, lambda m=msg: 
                                       self.document_processing_progress_label.configure(text=m))
                    
                    # Process the file
                    final_output_path = self.multi_part_post_processor.process_document_processing_from_ocr_json(
                        ocr_json_path=json_file_path,
                        user_prompt=document_prompt,
                        model_name=model_name,
                        book_id=book_id,
                        chapter_id=chapter_id_num,
                        start_point_index=start_point_index,
                        pointid_mapping_txt=pointid_mapping_txt,
                        progress_callback=progress_callback,
                    )
                    
                    if final_output_path and os.path.exists(final_output_path):
                        completed += 1
                        # Update status to completed
                        if hasattr(self, 'document_processing_file_info_list'):
                            for file_info in self.document_processing_file_info_list:
                                if file_info['file_path'] == json_file_path:
                                    file_info['output_path'] = final_output_path
                                    self.root.after(0, lambda sl=file_info['status_label']: 
                                                   sl.configure(text="Completed", text_color="green"))
                                    break
                        
                        self.logger.info(f"Successfully processed: {file_name} -> {os.path.basename(final_output_path)}")
                    else:
                        failed += 1
                        # Update status to failed
                        if hasattr(self, 'document_processing_file_info_list'):
                            for file_info in self.document_processing_file_info_list:
                                if file_info['file_path'] == json_file_path:
                                    self.root.after(0, lambda sl=file_info['status_label']: 
                                                   sl.configure(text="Failed", text_color="red"))
                                    break
                        self.logger.error(f"Failed to process: {file_name}")
                    
                    # Delay between files (except for last file)
                    if idx < total_files - 1 and delay_seconds > 0:
                        import time
                        time.sleep(delay_seconds)
                        
                except Exception as e:
                    failed += 1
                    self.logger.error(f"Error processing {file_name}: {str(e)}", exc_info=True)
                    # Update status to failed
                    if hasattr(self, 'document_processing_file_info_list'):
                        for file_info in self.document_processing_file_info_list:
                            if file_info['file_path'] == json_file_path:
                                self.root.after(0, lambda sl=file_info['status_label']: 
                                               sl.configure(text="Failed", text_color="red"))
                                break
            
            # Final progress update
            self.root.after(0, lambda: self.document_processing_progress_bar.set(1.0))
            self.root.after(0, lambda: self.document_processing_progress_label.configure(
                text=f"Completed: {completed} succeeded, {failed} failed"))
            
            # Show summary
            self.root.after(0, lambda: messagebox.showinfo(
                "Batch Processing Complete",
                f"Document Processing batch completed!\n\n"
                f"Total files: {total_files}\n"
                f"Successful: {completed}\n"
                f"Failed: {failed}"
            ))
            
        except Exception as e:
            self.logger.error(f"Error in batch Document Processing: {str(e)}", exc_info=True)
            self.root.after(0, lambda: messagebox.showerror("Error", f"Batch processing error:\n{str(e)}"))
        finally:
            try:
                self.root.after(
                    0,
                    lambda: start_button.configure(state="normal", text="Process All Files")
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
        # Force switch to Pre-OCR Topic Extraction tab (first tab) in tabview
        # Use after() to ensure the tabview is fully packed before setting tab
        def set_tab():
            try:
                self.main_tabview.set("Pre-OCR Topic Extraction")
                # Double-check after a short delay
                self.root.after(50, lambda: self.main_tabview.set("Pre-OCR Topic Extraction"))
            except Exception as e:
                self.logger.warning(f"Error setting tab to Pre-OCR Topic Extraction: {e}")
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
    
    def setup_ocr_extraction_ui(self, parent):
        """Setup UI for OCR Extraction"""
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
        title = ctk.CTkLabel(main_frame, text="OCR Extraction", 
                            font=ctk.CTkFont(size=24, weight="bold"))
        title.pack(pady=(0, 20))
        
        # Description
        desc = ctk.CTkLabel(
            main_frame, 
            text="Extract OCR content from PDF using topics from Pre-OCR Topic Extraction.",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        desc.pack(pady=(0, 20))
        
        # Batch processing section
        batch_frame = ctk.CTkFrame(main_frame)
        batch_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(batch_frame, text="Batch Processing:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        batch_btn_frame = ctk.CTkFrame(batch_frame)
        batch_btn_frame.pack(fill="x", padx=10, pady=5)
        
        # Option 1: Select folder and auto-match
        ctk.CTkButton(batch_btn_frame, text="Select Folder (Auto-Match)", 
                     command=self.select_folder_and_match_ocr_pairs, 
                     width=200).pack(side="left", padx=5)
        
        # Option 2: Manual add pairs (fallback)
        ctk.CTkButton(batch_btn_frame, text="Add Pair Manually", 
                     command=self.add_ocr_extraction_pair_manual, 
                     width=150).pack(side="left", padx=5)
        
        # Delay setting
        delay_frame = ctk.CTkFrame(batch_btn_frame)
        delay_frame.pack(side="left", padx=10)
        ctk.CTkLabel(delay_frame, text="Delay (seconds):").pack(side="left", padx=5)
        if not hasattr(self, 'ocr_extraction_delay_var'):
            self.ocr_extraction_delay_var = ctk.StringVar(value="5")
        delay_entry = ctk.CTkEntry(delay_frame, textvariable=self.ocr_extraction_delay_var, width=60)
        delay_entry.pack(side="left", padx=5)
        
        # Pairs list with status
        if not hasattr(self, 'ocr_extraction_pairs_list_frame'):
            self.ocr_extraction_pairs_list_frame = ctk.CTkFrame(main_frame)
        self.ocr_extraction_pairs_list_frame.pack(fill="both", expand=True, pady=10)
        
        ctk.CTkLabel(self.ocr_extraction_pairs_list_frame, text="Selected Pairs:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        # Scrollable frame for pairs list
        if not hasattr(self, 'ocr_extraction_pairs_list_scroll'):
            self.ocr_extraction_pairs_list_scroll = ctk.CTkScrollableFrame(self.ocr_extraction_pairs_list_frame, height=200)
        self.ocr_extraction_pairs_list_scroll.pack(fill="both", expand=True, padx=10, pady=5)
        
        # Progress bar
        if not hasattr(self, 'ocr_extraction_progress_bar'):
            self.ocr_extraction_progress_bar = ctk.CTkProgressBar(main_frame)
        self.ocr_extraction_progress_bar.pack(fill="x", padx=10, pady=5)
        self.ocr_extraction_progress_bar.set(0)
        
        # Progress label
        if not hasattr(self, 'ocr_extraction_progress_label'):
            self.ocr_extraction_progress_label = ctk.CTkLabel(main_frame, text="Ready", 
                                                             font=ctk.CTkFont(size=12))
        self.ocr_extraction_progress_label.pack(pady=5)
        
        # Variables for pairs tracking
        if not hasattr(self, 'ocr_extraction_selected_pairs'):
            self.ocr_extraction_selected_pairs = []
        
        # Prompt Section
        prompt_frame = ctk.CTkFrame(main_frame)
        prompt_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(
            prompt_frame,
            text="Prompt for OCR Extraction:",
            font=ctk.CTkFont(size=14, weight="bold"),
        ).pack(anchor="w", padx=10, pady=5)
        
        # OCR Extraction prompt mode (default vs custom)
        ocr_mode_frame = ctk.CTkFrame(prompt_frame)
        ocr_mode_frame.pack(fill="x", padx=10, pady=(0, 5))
        if not hasattr(self, 'ocr_extraction_prompt_type_var'):
            self.ocr_extraction_prompt_type_var = ctk.StringVar(value="default")
        ctk.CTkRadioButton(
            ocr_mode_frame,
            text="Use Default Prompt",
            variable=self.ocr_extraction_prompt_type_var,
            value="default",
            command=self.on_ocr_extraction_prompt_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)
        ctk.CTkRadioButton(
            ocr_mode_frame,
            text="Use Custom Prompt",
            variable=self.ocr_extraction_prompt_type_var,
            value="custom",
            command=self.on_ocr_extraction_prompt_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)
        
        # Default OCR Extraction prompt combobox
        ocr_default_frame = ctk.CTkFrame(prompt_frame)
        ocr_default_frame.pack(fill="x", padx=10, pady=(0, 10))
        
        ctk.CTkLabel(
            ocr_default_frame,
            text="Default OCR Extraction Prompt:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", pady=(0, 5))
        
        ocr_prompt_names = self.prompt_manager.get_prompt_names()
        preferred_ocr_name = "OCR Extraction Prompt"  # Use Document Processing prompt as default
        if preferred_ocr_name in ocr_prompt_names:
            ocr_default_value = preferred_ocr_name
        else:
            ocr_default_value = ocr_prompt_names[0] if ocr_prompt_names else ""
        if not hasattr(self, 'ocr_extraction_default_prompt_var'):
            self.ocr_extraction_default_prompt_var = ctk.StringVar(value=ocr_default_value)
        self.ocr_extraction_default_prompt_combo = ctk.CTkComboBox(
            ocr_default_frame,
            values=ocr_prompt_names,
            variable=self.ocr_extraction_default_prompt_var,
            width=400,
            command=self.on_ocr_extraction_default_prompt_selected,
        )
        self.ocr_extraction_default_prompt_combo.pack(anchor="w", pady=(0, 5))
        
        # Textbox for OCR Extraction prompt (default-filled or custom)
        if not hasattr(self, 'ocr_extraction_prompt_text'):
            self.ocr_extraction_prompt_text = ctk.CTkTextbox(prompt_frame, height=140, font=self.farsi_text_font)
        else:
            try:
                self.ocr_extraction_prompt_text.pack_forget()
            except Exception:
                pass
        self.ocr_extraction_prompt_text.pack(fill="x", padx=10, pady=(0, 10))
        
        # Load default prompt
        self.on_ocr_extraction_prompt_type_change()
        if hasattr(self, 'ocr_extraction_default_prompt_var'):
            default_prompt_name = self.ocr_extraction_default_prompt_var.get()
            if default_prompt_name:
                self.on_ocr_extraction_default_prompt_selected(default_prompt_name)
        
        # Model Selection
        model_frame = ctk.CTkFrame(main_frame)
        model_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(model_frame, text="Select Gemini Model:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'ocr_extraction_model_var'):
            self.ocr_extraction_model_var = ctk.StringVar(value="gemini-2.5-pro")
        ocr_model_combo = ctk.CTkComboBox(model_frame, values=APIConfig.TEXT_MODELS, 
                                         variable=self.ocr_extraction_model_var, width=400)
        ocr_model_combo.pack(anchor="w", padx=10, pady=(0, 10))
        
        # Process Button
        process_frame = ctk.CTkFrame(main_frame)
        process_frame.pack(fill="x", pady=10)
        
        # Process All button for batch processing
        if not hasattr(self, 'ocr_extraction_process_all_btn'):
            self.ocr_extraction_process_all_btn = ctk.CTkButton(
                process_frame,
                text="Process All Pairs",
                command=self.process_multiple_ocr_extractions,
                width=200,
                height=40,
                font=ctk.CTkFont(size=14, weight="bold"),
                fg_color="green",
                hover_color="darkgreen"
            )
        self.ocr_extraction_process_all_btn.pack(side="left", padx=10, pady=10)
        
        # Status Label
        if not hasattr(self, 'ocr_extraction_status_label'):
            self.ocr_extraction_status_label = ctk.CTkLabel(
                process_frame,
                text="Ready",
                font=ctk.CTkFont(size=12),
                text_color="gray"
            )
        self.ocr_extraction_status_label.pack(side="left", padx=10, pady=10)
        
        # Output File Path
        output_frame = ctk.CTkFrame(main_frame)
        output_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(output_frame, text="Output JSON File:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'ocr_extraction_output_var'):
            self.ocr_extraction_output_var = ctk.StringVar()
        output_entry = ctk.CTkEntry(output_frame, textvariable=self.ocr_extraction_output_var, 
                                    width=400, state="readonly")
        output_entry.pack(fill="x", padx=10, pady=(0, 10))
    
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
        title = ctk.CTkLabel(main_frame, text="Image Notes Generation", 
                            font=ctk.CTkFont(size=24, weight="bold"))
        title.pack(pady=(0, 20))
        
        # Description
        desc = ctk.CTkLabel(
            main_frame, 
            text="Generate image notes from Content Processing JSON and merge with Content Processing data.",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        desc.pack(pady=(0, 20))
        
        # File Pair Selection - Batch Processing
        pairs_frame = ctk.CTkFrame(main_frame)
        pairs_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(
            pairs_frame,
            text="Input File Pairs (Stage 4 JSON + OCR Extraction JSON) - Batch Processing",
            font=ctk.CTkFont(size=14, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        # File selection buttons
        buttons_frame = ctk.CTkFrame(pairs_frame)
        buttons_frame.pack(fill="x", padx=15, pady=5)
        
        def select_folder_and_match_stage_e_pairs():
            folder_path = filedialog.askdirectory(title="Select folder containing Stage 4 and OCR Extraction JSON files")
            if not folder_path:
                return
            
            import glob
            # Support both old format (Lesson_file_{book}_{chapter}.json) and new format (Lesson_file_{input_name}.json)
            stage4_files = glob.glob(os.path.join(folder_path, "Lesson_file_*.json"))
            stage4_files = [f for f in stage4_files if os.path.isfile(f)]
            
            ocr_files = glob.glob(os.path.join(folder_path, "*.json"))
            ocr_files = [f for f in ocr_files if os.path.isfile(f)]
            
            if not stage4_files:
                messagebox.showinfo("Info", "No Stage 4 JSON files (Lesson_file_*.json) found in selected folder")
                return
            
            if not ocr_files:
                messagebox.showinfo("Info", "No OCR Extraction JSON files found in selected folder")
                return
            
            # Match pairs based on book_id and chapter_id
            matched_pairs = []
            unmatched_stage4 = []
            
            for stage4_file in stage4_files:
                stage4_name = os.path.basename(stage4_file)
                matched_ocr = None
                
                # Strategy 1: Try to match by book_id and chapter_id from filename (old format: Lesson_file_{book}_{chapter}.json)
                import re
                match = re.search(r'Lesson_file_(\d+)_(\d+)\.json', stage4_name)
                if match:
                    book_id = int(match.group(1))
                    chapter_id = int(match.group(2))
                    
                    # Try to find matching OCR file by metadata
                    for ocr_file in ocr_files:
                        try:
                            with open(ocr_file, 'r', encoding='utf-8') as f:
                                ocr_data = json.load(f)
                            metadata = ocr_data.get('metadata', {})
                            ocr_book_id = metadata.get('book_id')
                            ocr_chapter_id = metadata.get('chapter_id')
                            
                            if ocr_book_id == book_id and ocr_chapter_id == chapter_id:
                                matched_ocr = ocr_file
                                break
                        except Exception:
                            continue
                
                # Strategy 2: Try to match by metadata from Stage 4 file (new format: Lesson_file_{input_name}.json)
                if not matched_ocr:
                    try:
                        with open(stage4_file, 'r', encoding='utf-8') as f:
                            stage4_data = json.load(f)
                        stage4_metadata = stage4_data.get('metadata', {})
                        stage4_book_id = stage4_metadata.get('book_id')
                        stage4_chapter_id = stage4_metadata.get('chapter_id')
                        
                        if stage4_book_id and stage4_chapter_id:
                            # Try to find matching OCR file by metadata
                            for ocr_file in ocr_files:
                                try:
                                    with open(ocr_file, 'r', encoding='utf-8') as f:
                                        ocr_data = json.load(f)
                                    ocr_metadata = ocr_data.get('metadata', {})
                                    ocr_book_id = ocr_metadata.get('book_id')
                                    ocr_chapter_id = ocr_metadata.get('chapter_id')
                                    
                                    if (ocr_book_id == stage4_book_id and 
                                        ocr_chapter_id == stage4_chapter_id):
                                        matched_ocr = ocr_file
                                        break
                                except Exception:
                                    continue
                    except Exception:
                        pass
                
                if matched_ocr:
                    matched_pairs.append((stage4_file, matched_ocr))
                else:
                    unmatched_stage4.append(stage4_file)
            
            if not matched_pairs:
                messagebox.showinfo("Info", "No matching pairs found. Please check file names or metadata.")
                return
            
            # Add pairs to UI
            if not hasattr(self, 'stage_e_selected_pairs'):
                self.stage_e_selected_pairs = []
            
            added_count = 0
            for stage4_path, ocr_path in matched_pairs:
                # Check if pair already exists
                exists = any(
                    p['stage4_path'] == stage4_path and p['ocr_path'] == ocr_path
                    for p in self.stage_e_selected_pairs
                )
                if not exists:
                    self.stage_e_selected_pairs.append({
                        'stage4_path': stage4_path,
                        'ocr_path': ocr_path,
                        'status': 'pending',
                        'output_path': None
                    })
                    self._add_stage_e_pair_to_ui(stage4_path, ocr_path)
                    added_count += 1
            
            summary = f"Auto-matched {added_count} pair(s):\n"
            for stage4_path, ocr_path in matched_pairs[:5]:
                summary += f"\nStage4: {os.path.basename(stage4_path)}\nOCR: {os.path.basename(ocr_path)}"
            if len(matched_pairs) > 5:
                summary += f"\n... and {len(matched_pairs) - 5} more"
            
            if unmatched_stage4:
                summary += f"\n\nUnmatched Stage 4 files: {len(unmatched_stage4)}"
            
            messagebox.showinfo("Auto-Matching Results", summary)
            self.logger.info(f"Auto-matched {added_count} pairs from folder: {folder_path}")
        
        def add_stage_e_pair_manual():
            # Browse Stage 4 JSON
            stage4_path = filedialog.askopenfilename(
                title="Select Stage 4 JSON File (Content Processing)",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            if not stage4_path:
                return
            
            # Browse OCR Extraction JSON
            ocr_path = filedialog.askopenfilename(
                title="Select OCR Extraction JSON File",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            if not ocr_path:
                return
            
            if not hasattr(self, 'stage_e_selected_pairs'):
                self.stage_e_selected_pairs = []
            
            # Check if pair already exists
            exists = any(
                p['stage4_path'] == stage4_path and p['ocr_path'] == ocr_path
                for p in self.stage_e_selected_pairs
            )
            if not exists:
                self.stage_e_selected_pairs.append({
                    'stage4_path': stage4_path,
                    'ocr_path': ocr_path,
                    'status': 'pending',
                    'output_path': None
                })
                self._add_stage_e_pair_to_ui(stage4_path, ocr_path)
                self.logger.info(f"Added manual pair: Stage4={os.path.basename(stage4_path)}, OCR={os.path.basename(ocr_path)}")
        
        ctk.CTkButton(
            buttons_frame,
            text="Select Folder (Auto-Match)",
            command=select_folder_and_match_stage_e_pairs,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        ctk.CTkButton(
            buttons_frame,
            text="Add Pair Manually",
            command=add_stage_e_pair_manual,
            width=180,
        ).pack(side="left", padx=5, pady=5)
        
        # Delay setting
        delay_frame = ctk.CTkFrame(pairs_frame)
        delay_frame.pack(fill="x", padx=15, pady=5)
        
        ctk.CTkLabel(
            delay_frame,
            text="Delay between pairs (seconds):",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(side="left", padx=10, pady=5)
        
        if not hasattr(self, 'stage_e_delay_var'):
            self.stage_e_delay_var = ctk.StringVar(value="5")
        
        delay_entry = ctk.CTkEntry(delay_frame, textvariable=self.stage_e_delay_var, width=100)
        delay_entry.pack(side="left", padx=5, pady=5)
        
        # Pairs list (scrollable)
        list_frame = ctk.CTkFrame(pairs_frame)
        list_frame.pack(fill="both", expand=True, padx=15, pady=5)
        
        ctk.CTkLabel(
            list_frame,
            text="Selected Pairs:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_e_pairs_list_scroll'):
            self.stage_e_pairs_list_scroll = ctk.CTkScrollableFrame(list_frame, height=150)
        self.stage_e_pairs_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize selected pairs list if not exists
        if not hasattr(self, 'stage_e_selected_pairs'):
            self.stage_e_selected_pairs = []
        
        ctk.CTkLabel(
            pairs_frame,
            text="Each pair should have: Stage 4 JSON (with PointId) + OCR Extraction JSON",
            font=ctk.CTkFont(size=10),
            text_color="gray",
        ).pack(anchor="w", padx=15, pady=(0, 10))
        
        # Prompt Section
        prompt_frame = ctk.CTkFrame(main_frame)
        prompt_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(
            prompt_frame,
            text="Prompt for Image Notes Generation:",
            font=ctk.CTkFont(size=14, weight="bold"),
        ).pack(anchor="w", padx=10, pady=5)

        # Stage E prompt mode (default vs custom)
        stage_e_mode_frame = ctk.CTkFrame(prompt_frame)
        stage_e_mode_frame.pack(fill="x", padx=10, pady=(0, 5))
        if not hasattr(self, 'stage_e_prompt_type_var'):
            self.stage_e_prompt_type_var = ctk.StringVar(value="default")
        ctk.CTkRadioButton(
            stage_e_mode_frame,
            text="Use Default Prompt",
            variable=self.stage_e_prompt_type_var,
            value="default",
            command=self.on_stage_e_prompt_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)
        ctk.CTkRadioButton(
            stage_e_mode_frame,
            text="Use Custom Prompt",
            variable=self.stage_e_prompt_type_var,
            value="custom",
            command=self.on_stage_e_prompt_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)

        # Default Stage E prompt combobox (predefined prompts)
        stage_e_default_frame = ctk.CTkFrame(prompt_frame)
        stage_e_default_frame.pack(fill="x", padx=10, pady=(0, 10))

        ctk.CTkLabel(
            stage_e_default_frame,
            text="Default Stage E Prompt:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", pady=(0, 5))

        stage_e_prompt_names = self.prompt_manager.get_prompt_names()
        # Prefer the Image Notes prompt by its current name in prompts.json
        preferred_stage_e_name = "Image Notes Prompt"
        if preferred_stage_e_name in stage_e_prompt_names:
            stage_e_default_value = preferred_stage_e_name
        else:
            stage_e_default_value = stage_e_prompt_names[0] if stage_e_prompt_names else ""
        self.stage_e_default_prompt_var = ctk.StringVar(value=stage_e_default_value)
        self.stage_e_default_prompt_combo = ctk.CTkComboBox(
            stage_e_default_frame,
            values=stage_e_prompt_names,
            variable=self.stage_e_default_prompt_var,
            width=400,
            command=self.on_stage_e_default_prompt_selected,
        )
        self.stage_e_default_prompt_combo.pack(anchor="w", pady=(0, 5))

        # Textbox for Stage E prompt (default-filled or custom)
        if not hasattr(self, 'stage_e_prompt_text'):
            self.stage_e_prompt_text = ctk.CTkTextbox(prompt_frame, height=150, font=self.farsi_text_font)
        else:
            try:
                self.stage_e_prompt_text.pack_forget()
            except Exception:
                pass
        self.stage_e_prompt_text.pack(fill="x", padx=10, pady=5)
        # Apply initial default/custom state and fill textbox
        self.on_stage_e_prompt_type_change()
        
        # Model Selection
        model_frame = ctk.CTkFrame(main_frame)
        model_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(model_frame, text="Model:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        # Get default model from main model selection if available
        default_model = "gemini-2.5-pro"
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
        
        # Process Buttons
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)
        
        # Single file processing button (for backward compatibility)
        self.stage_e_process_btn = ctk.CTkButton(
            process_btn_frame,
            text="Process Single Pair",
            command=self.process_stage_e,
            width=200,
            height=40,
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color="blue"
        )
        self.stage_e_process_btn.pack(side="left", padx=10, pady=10)
        
        # Batch processing button
        def start_batch_processing():
            if not hasattr(self, 'stage_e_selected_pairs') or not self.stage_e_selected_pairs:
                messagebox.showwarning("Warning", "Please add at least one file pair")
                return
            batch_btn.configure(state="disabled", text="Processing...")
            self.full_pipeline_cancel = False
            threading.Thread(
                target=self.process_multiple_stage_e_pairs,
                args=(self.root, batch_btn),
                daemon=True,
            ).start()
        
        batch_btn = ctk.CTkButton(
            process_btn_frame,
            text="Process All Pairs",
            command=start_batch_processing,
            width=200,
            height=40,
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color="green",
            hover_color="darkgreen"
        )
        batch_btn.pack(side="left", padx=10, pady=10)
        self.stage_e_process_all_btn = batch_btn
        
        # View CSV button
        self.stage_e_view_csv_btn = ctk.CTkButton(
            process_btn_frame,
            text="View CSV",
            command=self.view_csv_stage_e,
            width=150,
            height=40,
            font=ctk.CTkFont(size=14),
            fg_color="purple",
            hover_color="darkpurple",
            state="disabled"
        )
        self.stage_e_view_csv_btn.pack(side="left", padx=10, pady=10)
        
        # Progress bar for batch processing
        progress_frame = ctk.CTkFrame(main_frame)
        progress_frame.pack(fill="x", padx=20, pady=10)
        
        if not hasattr(self, 'stage_e_progress_bar'):
            self.stage_e_progress_bar = ctk.CTkProgressBar(progress_frame)
        self.stage_e_progress_bar.pack(fill="x", padx=10, pady=(0, 5))
        self.stage_e_progress_bar.set(0.0)
        
        if not hasattr(self, 'stage_e_progress_label'):
            self.stage_e_progress_label = ctk.CTkLabel(
                progress_frame,
                text="Waiting to start...",
                font=ctk.CTkFont(size=10),
                text_color="gray",
            )
        self.stage_e_progress_label.pack(anchor="w", padx=10, pady=(0, 5))
        
        # Status for Stage E
        self.stage_e_status_label = ctk.CTkLabel(main_frame, text="Ready", 
                                                 font=ctk.CTkFont(size=12), text_color="gray")
        self.stage_e_status_label.pack(pady=10)
        
        # Note: Auto-validation removed since we're using batch processing with pairs list
        # Single file selection variables (stage_e_stage4_var, stage_e_ocr_extraction_json_var) 
        # are no longer used in batch processing mode, but kept for backward compatibility
        # Initialize them as empty if they don't exist (for backward compatibility)
        if not hasattr(self, 'stage_e_stage4_var'):
            self.stage_e_stage4_var = ctk.StringVar(value="")
        if not hasattr(self, 'stage_e_ocr_extraction_json_var'):
            self.stage_e_ocr_extraction_json_var = ctk.StringVar(value="")
    
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
        """Validate Stage E input files (for backward compatibility with single file mode)"""
        # Check if single file mode variables exist (for backward compatibility)
        if not hasattr(self, 'stage_e_stage4_var'):
            return  # Batch processing mode - no single file validation needed
        
        stage4_path = self.stage_e_stage4_var.get()
        ocr_extraction_path = self.stage_e_ocr_extraction_json_var.get() if hasattr(self, 'stage_e_ocr_extraction_json_var') else ""
        
        # Validate Stage 4 (only if validation labels exist)
        if hasattr(self, 'stage_e_stage4_valid'):
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
        
        # Validate OCR Extraction JSON (only if validation labels exist)
        if hasattr(self, 'stage_e_ocr_extraction_valid'):
            if ocr_extraction_path:
                if os.path.exists(ocr_extraction_path):
                    if ocr_extraction_path.lower().endswith('.json'):
                        self.stage_e_ocr_extraction_valid.configure(text="OK", text_color="green")
                    else:
                        self.stage_e_ocr_extraction_valid.configure(text="W", text_color="orange")
                else:
                    self.stage_e_ocr_extraction_valid.configure(text="X", text_color="red")
            else:
                self.stage_e_ocr_extraction_valid.configure(text="", text_color="gray")
    
    def process_stage_e(self):
        """Process Stage E in background thread"""
        def worker():
            try:
                self.stage_e_process_btn.configure(state="disabled", text="Processing...")
                self.update_stage_status("E", "processing")
                self.stage_e_status_label.configure(text="Processing Image Notes Generation...", text_color="blue")
                
                # Validate inputs (for single file mode - backward compatibility)
                if not hasattr(self, 'stage_e_stage4_var'):
                    messagebox.showerror("Error", "Single file mode not available. Please use batch processing mode (Process All Pairs).")
                    return
                
                stage4_path = self.stage_e_stage4_var.get().strip()
                ocr_extraction_json_path = self.stage_e_ocr_extraction_json_var.get().strip() if hasattr(self, 'stage_e_ocr_extraction_json_var') else ""
                prompt = self.stage_e_prompt_text.get("1.0", tk.END).strip()
                # Always use default model from main view settings
                model_name = self.get_default_model()
                
                if not stage4_path or not os.path.exists(stage4_path):
                    messagebox.showerror("Error", "Please select a valid Content Processing JSON file")
                    return
                
                if not ocr_extraction_json_path or not os.path.exists(ocr_extraction_json_path):
                    messagebox.showerror("Error", "Please select a valid OCR Extraction JSON file")
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
                
                # Process Stage E (use default output dir from main view)
                output_path = self.stage_e_processor.process_stage_e(
                    stage4_path=stage4_path,
                    ocr_extraction_json_path=ocr_extraction_json_path,
                    prompt=prompt,
                    model_name=model_name,
                    output_dir=self.get_default_output_dir(stage4_path),
                    progress_callback=progress_callback
                )
                
                if output_path:
                    self.last_stage_e_path = output_path
                    self.update_stage_status("E", "completed", output_path)
                    self.root.after(0, lambda: self.stage_e_view_csv_btn.configure(state="normal"))
                    self.stage_e_status_label.configure(
                        text=f"Stage E completed successfully!\nOutput: {os.path.basename(output_path)}",
                        text_color="green"
                    )
                    messagebox.showinfo("Success", f"Image Notes Generation completed!\n\nOutput saved to:\n{output_path}")
                else:
                    self.update_stage_status("E", "error")
                    self.stage_e_status_label.configure(text="Image Notes Generation failed. Check logs for details.", text_color="red")
                    messagebox.showerror("Error", "Image Notes Generation processing failed. Check logs for details.")
            
            except Exception as e:
                self.logger.error(f"Error in Image Notes Generation processing: {e}", exc_info=True)
                self.update_stage_status("E", "error")
                self.stage_e_status_label.configure(text=f"Error: {str(e)}", text_color="red")
                messagebox.showerror("Error", f"Image Notes Generation processing error:\n{str(e)}")
            finally:
                self.root.after(0, lambda: self.stage_e_process_btn.configure(state="normal", text="Process Single Pair"))
        
        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def _add_stage_e_pair_to_ui(self, stage4_path: str, ocr_path: str):
        """Add a file pair to the Stage E UI list"""
        pair_frame = ctk.CTkFrame(self.stage_e_pairs_list_scroll)
        pair_frame.pack(fill="x", padx=5, pady=2)
        
        # Stage 4 file name
        stage4_name = os.path.basename(stage4_path)
        stage4_label = ctk.CTkLabel(pair_frame, text=f"Stage4: {stage4_name}", 
                                   font=ctk.CTkFont(size=10))
        stage4_label.pack(side="left", padx=5)
        
        # OCR file name
        ocr_name = os.path.basename(ocr_path)
        ocr_label = ctk.CTkLabel(pair_frame, text=f"OCR: {ocr_name}", 
                                font=ctk.CTkFont(size=10))
        ocr_label.pack(side="left", padx=5)
        
        # Status label
        status_label = ctk.CTkLabel(pair_frame, text="Pending", 
                                   text_color="gray", font=ctk.CTkFont(size=11))
        status_label.pack(side="right", padx=5)
        
        # Remove button
        remove_btn = ctk.CTkButton(pair_frame, text="X", width=30, height=20,
                                  command=lambda s4=stage4_path, ocr=ocr_path, w=pair_frame: 
                                  self.remove_stage_e_pair(s4, ocr, w))
        remove_btn.pack(side="right", padx=2)
        
        # Store pair info (if list doesn't exist, create it)
        if not hasattr(self, 'stage_e_pair_info_list'):
            self.stage_e_pair_info_list = []
        
        self.stage_e_pair_info_list.append({
            'stage4_path': stage4_path,
            'ocr_path': ocr_path,
            'status_label': status_label,
            'frame': pair_frame,
            'output_path': None
        })
    
    def remove_stage_e_pair(self, stage4_path: str, ocr_path: str, frame_widget):
        """Remove a pair from the Stage E selection list"""
        if hasattr(self, 'stage_e_selected_pairs'):
            self.stage_e_selected_pairs = [
                p for p in self.stage_e_selected_pairs 
                if not (p['stage4_path'] == stage4_path and p['ocr_path'] == ocr_path)
            ]
        if hasattr(self, 'stage_e_pair_info_list'):
            self.stage_e_pair_info_list = [
                p for p in self.stage_e_pair_info_list
                if not (p['stage4_path'] == stage4_path and p['ocr_path'] == ocr_path)
            ]
        frame_widget.destroy()
    
    def process_multiple_stage_e_pairs(self, parent_window, start_button):
        """Process multiple Stage E file pairs sequentially"""
        try:
            if not hasattr(self, 'stage_e_selected_pairs') or not self.stage_e_selected_pairs:
                self.root.after(0, lambda: messagebox.showwarning("Warning", "Please add at least one file pair"))
                return
            
            # Get prompt
            stage_e_prompt = self.stage_e_prompt_text.get("1.0", tk.END).strip()
            if not stage_e_prompt:
                default_prompt = self.prompt_manager.get_prompt("Image Notes Prompt")
                if default_prompt:
                    stage_e_prompt = default_prompt
                    self.logger.info("Using default Image Notes prompt from prompts.json")
                else:
                    self.root.after(0, lambda: messagebox.showerror("Error", "Please enter a prompt"))
                    return
            
            # Get delay
            try:
                delay_seconds = int(self.stage_e_delay_var.get())
            except ValueError:
                delay_seconds = 5
                self.logger.warning(f"Invalid delay value, using default: {delay_seconds} seconds")
            
            # Validate API keys
            if not self.api_key_manager.api_keys:
                self.root.after(0, lambda: messagebox.showerror("Error", "Please load API keys first"))
                return
            
            # Get model
            model_name = self.get_default_model()
            total_pairs = len(self.stage_e_selected_pairs)
            completed = 0
            failed = 0
            skipped = 0
            
            # Reset progress bar
            self.root.after(0, lambda: self.stage_e_progress_bar.set(0))
            
            # Process each pair
            for idx, pair in enumerate(self.stage_e_selected_pairs):
                if self.full_pipeline_cancel:
                    self.root.after(0, lambda: self.stage_e_progress_label.configure(
                        text="Cancelled by user", text_color="orange"))
                    break
                
                stage4_path = pair['stage4_path']
                ocr_path = pair['ocr_path']
                stage4_name = os.path.basename(stage4_path)
                ocr_name = os.path.basename(ocr_path)
                
                # Check if output file already exists (check for incomplete/remaining files)
                output_dir = self.get_default_output_dir(stage4_path)
                expected_output_path = None
                
                # Try to determine expected output filename by reading Stage 4 file
                # Format: e{book}{chapter}_{base_name}.json (e.g., e105003_Lesson_file_1_1.json)
                stage4_data = None
                base_name_stage4, _ = os.path.splitext(stage4_name)
                try:
                    with open(stage4_path, 'r', encoding='utf-8') as f:
                        stage4_data = json.load(f)
                    stage4_points = stage4_data.get("data") or stage4_data.get("points") or stage4_data.get("rows", [])
                    if stage4_points and stage4_points[0].get("PointId"):
                        first_point_id = stage4_points[0].get("PointId")
                        book_id, chapter_id = self.stage_e_processor.extract_book_chapter_from_pointid(first_point_id)
                        # Use unique filename format: e{book}{chapter}_{base_name}.json
                        output_filename = f"e{book_id:03d}{chapter_id:03d}_{base_name_stage4}.json"
                        expected_output_path = os.path.join(output_dir, output_filename)
                except Exception as e:
                    self.logger.warning(f"Could not determine expected output path for {stage4_name}: {e}")
                    # Fallback: try common output directory (same directory as Stage 4 file)
                    base_dir = os.path.dirname(stage4_path) or os.getcwd()
                    try:
                        # Read Stage 4 file again if not already loaded
                        if stage4_data is None:
                            with open(stage4_path, 'r', encoding='utf-8') as f:
                                stage4_data = json.load(f)
                            stage4_points = stage4_data.get("data") or stage4_data.get("points") or stage4_data.get("rows", [])
                        else:
                            stage4_points = stage4_data.get("data") or stage4_data.get("points") or stage4_data.get("rows", [])
                        if stage4_points and stage4_points[0].get("PointId"):
                            first_point_id = stage4_points[0].get("PointId")
                            book_id, chapter_id = self.stage_e_processor.extract_book_chapter_from_pointid(first_point_id)
                            # Use unique filename format: e{book}{chapter}_{base_name}.json
                            output_filename = f"e{book_id:03d}{chapter_id:03d}_{base_name_stage4}.json"
                            expected_output_path = os.path.join(base_dir, output_filename)
                    except:
                        pass
                
                # Check if output already exists and is valid
                if expected_output_path and os.path.exists(expected_output_path):
                    try:
                        # Validate that the file is a complete Stage E output
                        with open(expected_output_path, 'r', encoding='utf-8') as f:
                            existing_data = json.load(f)
                        # Check if it has the expected structure (data field with points)
                        existing_points = existing_data.get("data") or existing_data.get("points") or existing_data.get("rows", [])
                        if existing_points and len(existing_points) > 0:
                            # File exists and appears complete, skip it
                            skipped += 1
                            self.logger.info(f"Skipping already processed pair: {stage4_name} + {ocr_name} -> {os.path.basename(expected_output_path)}")
                            
                            # Update status to skipped/already completed
                            if hasattr(self, 'stage_e_pair_info_list'):
                                for pair_info in self.stage_e_pair_info_list:
                                    if pair_info['stage4_path'] == stage4_path and pair_info['ocr_path'] == ocr_path:
                                        pair_info['output_path'] = expected_output_path
                                        self.root.after(0, lambda sl=pair_info['status_label']: 
                                                       sl.configure(text="Already Completed", text_color="gray"))
                                        break
                            
                            # Update progress
                            self.root.after(0, lambda idx=idx, total=total_pairs, s4=stage4_name: 
                                           self.stage_e_progress_label.configure(
                                               text=f"Skipping pair {idx+1}/{total}: {s4} (already completed)"))
                            
                            # Progress bar
                            progress = (idx + 1) / total_pairs
                            self.root.after(0, lambda p=progress: self.stage_e_progress_bar.set(p))
                            
                            # Continue to next pair
                            continue
                    except Exception as e:
                        self.logger.warning(f"Existing output file {expected_output_path} appears invalid, will reprocess: {e}")
                        # File exists but is invalid, continue to process it
                
                # Update status to processing
                if hasattr(self, 'stage_e_pair_info_list'):
                    for pair_info in self.stage_e_pair_info_list:
                        if pair_info['stage4_path'] == stage4_path and pair_info['ocr_path'] == ocr_path:
                            self.root.after(0, lambda sl=pair_info['status_label']: 
                                           sl.configure(text="Processing...", text_color="blue"))
                            break
                
                self.root.after(0, lambda idx=idx, total=total_pairs, s4=stage4_name: 
                               self.stage_e_progress_label.configure(
                                   text=f"Processing pair {idx+1}/{total}: {s4}"))
                
                # Progress bar
                progress = idx / total_pairs
                self.root.after(0, lambda p=progress: self.stage_e_progress_bar.set(p))
                
                try:
                    # Progress callback for this pair
                    def progress_callback(msg: str):
                        self.root.after(0, lambda m=msg: 
                                       self.stage_e_progress_label.configure(text=m))
                    
                    # Process the pair
                    output_path = self.stage_e_processor.process_stage_e(
                        stage4_path=stage4_path,
                        ocr_extraction_json_path=ocr_path,
                        prompt=stage_e_prompt,
                        model_name=model_name,
                        output_dir=output_dir,
                        progress_callback=progress_callback
                    )
                    
                    if output_path and os.path.exists(output_path):
                        completed += 1
                        # Update status to completed
                        if hasattr(self, 'stage_e_pair_info_list'):
                            for pair_info in self.stage_e_pair_info_list:
                                if pair_info['stage4_path'] == stage4_path and pair_info['ocr_path'] == ocr_path:
                                    pair_info['output_path'] = output_path
                                    self.root.after(0, lambda sl=pair_info['status_label']: 
                                                   sl.configure(text="Completed", text_color="green"))
                                    break
                        
                        self.logger.info(f"Successfully processed: {stage4_name} + {ocr_name} -> {os.path.basename(output_path)}")
                    else:
                        failed += 1
                        # Update status to failed
                        if hasattr(self, 'stage_e_pair_info_list'):
                            for pair_info in self.stage_e_pair_info_list:
                                if pair_info['stage4_path'] == stage4_path and pair_info['ocr_path'] == ocr_path:
                                    self.root.after(0, lambda sl=pair_info['status_label']: 
                                                   sl.configure(text="Failed", text_color="red"))
                                    break
                        self.logger.error(f"Failed to process: {stage4_name} + {ocr_name}")
                    
                    # Delay between pairs (except for last pair)
                    if idx < total_pairs - 1 and delay_seconds > 0:
                        import time
                        time.sleep(delay_seconds)
                        
                except Exception as e:
                    failed += 1
                    self.logger.error(f"Error processing {stage4_name} + {ocr_name}: {str(e)}", exc_info=True)
                    # Update status to failed
                    if hasattr(self, 'stage_e_pair_info_list'):
                        for pair_info in self.stage_e_pair_info_list:
                            if pair_info['stage4_path'] == stage4_path and pair_info['ocr_path'] == ocr_path:
                                self.root.after(0, lambda sl=pair_info['status_label']: 
                                               sl.configure(text="Failed", text_color="red"))
                                break
            
            # Final progress update
            self.root.after(0, lambda: self.stage_e_progress_bar.set(1.0))
            self.root.after(0, lambda: self.stage_e_progress_label.configure(
                text=f"Completed: {completed} succeeded, {failed} failed, {skipped} skipped"))
            
            # Show summary
            summary_msg = f"Image Notes Generation batch completed!\n\n"
            summary_msg += f"Total pairs: {total_pairs}\n"
            summary_msg += f"Successful: {completed}\n"
            if skipped > 0:
                summary_msg += f"Skipped (already completed): {skipped}\n"
            summary_msg += f"Failed: {failed}"
            
            self.root.after(0, lambda: messagebox.showinfo(
                "Batch Processing Complete",
                summary_msg
            ))
            
        except Exception as e:
            self.logger.error(f"Error in batch Image Notes Generation: {str(e)}", exc_info=True)
            self.root.after(0, lambda: messagebox.showerror("Error", f"Batch processing error:\n{str(e)}"))
        finally:
            try:
                self.root.after(
                    0,
                    lambda: start_button.configure(state="normal", text="Process All Pairs")
                )
            except Exception:
                pass
    
    def process_ocr_extraction(self):
        """Process OCR Extraction in background thread"""
        def worker():
            try:
                self.ocr_extraction_process_btn.configure(state="disabled", text="Processing...")
                self.ocr_extraction_status_label.configure(text="Processing OCR Extraction...", text_color="blue")
                
                # Validate inputs
                pdf_path = self.ocr_extraction_pdf_var.get().strip()
                topic_file_path = self.ocr_extraction_topic_var.get().strip()
                prompt = self.ocr_extraction_prompt_text.get("1.0", tk.END).strip()
                # Always use default model from main view settings
                model_name = self.get_default_model()
                
                if not pdf_path or not os.path.exists(pdf_path):
                    self.ocr_extraction_status_label.configure(text="Error: Please select a valid PDF file", text_color="red")
                    messagebox.showerror("Error", "Please select a valid PDF file")
                    return
                
                if not topic_file_path or not os.path.exists(topic_file_path):
                    self.ocr_extraction_status_label.configure(text="Error: Please select a valid Pre-OCR Topic file (t*.json)", text_color="red")
                    messagebox.showerror("Error", "Please select a valid Pre-OCR Topic file (t*.json)")
                    return
                
                if not prompt:
                    self.ocr_extraction_status_label.configure(text="Error: Please enter a prompt", text_color="red")
                    messagebox.showerror("Error", "Please enter a prompt for OCR extraction")
                    return
                
                # Validate API keys
                if not self.api_key_manager.api_keys:
                    self.ocr_extraction_status_label.configure(text="Error: Please load API keys first", text_color="red")
                    messagebox.showerror("Error", "Please load API keys first")
                    return
                
                # Load topic file to get book_id and chapter_id (for reference, not used in processing yet)
                try:
                    with open(topic_file_path, 'r', encoding='utf-8') as f:
                        topic_data = json.load(f)
                    metadata = topic_data.get('metadata', {})
                    book_id = metadata.get('book_id', 105)
                    chapter_id = metadata.get('chapter_id', 3)
                    self.logger.info(f"Using topic file: {topic_file_path}, Book ID: {book_id}, Chapter ID: {chapter_id}")
                except Exception as e:
                    self.logger.warning(f"Could not extract book/chapter from topic file: {e}")
                    book_id = 105
                    chapter_id = 3
                
                def progress_callback(msg: str):
                    self.root.after(0, lambda: self.ocr_extraction_status_label.configure(text=msg))
                
                # Process OCR Extraction using multi_part_processor with topics
                # For each Subchapter, send PDF + prompt (with topics list) to model
                # Use default output dir from main view
                output_dir = self.get_default_output_dir(pdf_path)
                
                final_output_path = self.multi_part_processor.process_ocr_extraction_with_topics(
                    pdf_path=pdf_path,
                    topic_file_path=topic_file_path,
                    base_prompt=prompt,
                    model_name=model_name,
                    temperature=0.7,
                    progress_callback=progress_callback,
                    output_dir=output_dir
                )
                
                if final_output_path and os.path.exists(final_output_path):
                    # Update output path display
                    self.ocr_extraction_output_var.set(final_output_path)
                    self.ocr_extraction_status_label.configure(
                        text=f"OCR Extraction completed successfully!\nOutput: {os.path.basename(final_output_path)}",
                        text_color="green"
                    )
                    messagebox.showinfo("Success", f"OCR Extraction completed!\n\nOutput saved to:\n{final_output_path}")
                else:
                    self.ocr_extraction_status_label.configure(text="OCR Extraction failed. Check logs for details.", text_color="red")
                    messagebox.showerror("Error", "OCR Extraction processing failed. Check logs for details.")
            
            except Exception as e:
                self.logger.error(f"Error in OCR Extraction processing: {e}", exc_info=True)
                self.ocr_extraction_status_label.configure(text=f"Error: {str(e)}", text_color="red")
                messagebox.showerror("Error", f"OCR Extraction processing error:\n{str(e)}")
            finally:
                self.ocr_extraction_process_btn.configure(state="normal", text="Extract OCR")
        
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def select_folder_and_match_ocr_pairs(self):
        """Select a folder and automatically match PDFs with Topic files"""
        folder_path = filedialog.askdirectory(title="Select Folder Containing PDFs and Topic Files")
        if not folder_path:
            return
        
        # Find all PDFs and Topic files
        pdf_files = []
        topic_files = []
        
        for file in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file)
            if os.path.isfile(file_path):
                if file.lower().endswith('.pdf'):
                    pdf_files.append(file_path)
                elif file.lower().endswith('.json') and file.startswith('t') and len(file) >= 10:
                    # Topic file: t{book}{chapter}*.json (supports both old and new formats)
                    topic_files.append(file_path)
        
        if not pdf_files:
            messagebox.showwarning("Warning", "No PDF files found in selected folder")
            return
        
        if not topic_files:
            messagebox.showwarning("Warning", "No Topic files (t*.json) found in selected folder")
            return
        
        # Clear existing pairs
        self.ocr_extraction_selected_pairs = []
        for widget in self.ocr_extraction_pairs_list_scroll.winfo_children():
            widget.destroy()
        
        # Match PDFs with Topic files
        matched_pairs = []
        unmatched_pdfs = []
        
        for pdf_path in pdf_files:
            pdf_name = os.path.basename(pdf_path)
            pdf_name_without_ext = os.path.splitext(pdf_name)[0]
            matched_topic = None
            
            # Strategy 1: Match by PDF name suffix (most reliable for new format)
            # Look for topic file ending with: _{pdf_name_without_ext}.json
            expected_suffix = f"_{pdf_name_without_ext}.json"
            for topic_path in topic_files:
                topic_basename = os.path.basename(topic_path)
                if topic_basename.endswith(expected_suffix) and topic_basename.startswith('t'):
                    matched_topic = topic_path
                    break
            
            # Strategy 2: Try to extract book_id and chapter_id from PDF filename
            # Then match with pattern t{book:03d}{chapter:03d}_{pdf_name}.json
            if not matched_topic:
                book_id, chapter_id = self._extract_book_chapter_from_pdf_name(pdf_name)
                if book_id is not None and chapter_id is not None:
                    expected_prefix = f"t{book_id:03d}{chapter_id:03d}"
                    expected_format = f"{expected_prefix}_{pdf_name_without_ext}.json"
                    for topic_path in topic_files:
                        topic_basename = os.path.basename(topic_path)
                        if topic_basename == expected_format:
                            matched_topic = topic_path
                            break
                    
                    # If exact match not found, try prefix match (old format)
                    if not matched_topic:
                        for topic_path in topic_files:
                            topic_basename = os.path.basename(topic_path)
                            if topic_basename.startswith(expected_prefix) and topic_basename.endswith('.json'):
                                matched_topic = topic_path
                                break
            
            # Strategy 3: Try matching by metadata as fallback
            if not matched_topic:
                matched_topic = self._find_matching_topic_file_by_metadata(pdf_path, topic_files)
            
            if matched_topic:
                matched_pairs.append((pdf_path, matched_topic))
            else:
                unmatched_pdfs.append(pdf_path)
        
        # Add matched pairs to UI
        for pdf_path, topic_path in matched_pairs:
            self._add_ocr_pair_to_ui(pdf_path, topic_path)
        
        # Show summary
        summary = f"Auto-Matching Complete!\n\n" \
                 f"PDFs found: {len(pdf_files)}\n" \
                 f"Topic files found: {len(topic_files)}\n" \
                 f"Matched pairs: {len(matched_pairs)}\n" \
                 f"Unmatched PDFs: {len(unmatched_pdfs)}"
        
        if unmatched_pdfs:
            summary += f"\n\nUnmatched PDFs:\n" + "\n".join([os.path.basename(p) for p in unmatched_pdfs[:5]])
            if len(unmatched_pdfs) > 5:
                summary += f"\n... and {len(unmatched_pdfs) - 5} more"
        
        messagebox.showinfo("Auto-Matching Results", summary)
        
        self.logger.info(f"Auto-matched {len(matched_pairs)} pairs from folder: {folder_path}")
    
    def _extract_book_chapter_from_pdf_name(self, pdf_name: str) -> tuple[Optional[int], Optional[int]]:
        """Extract book_id and chapter_id from PDF filename"""
        import re
        # Try patterns like: book5_chapter3, b5c3, 5_3, etc.
        patterns = [
            r'book[_\s]*(\d+)[_\s]*chapter[_\s]*(\d+)',
            r'b[_\s]*(\d+)[_\s]*c[_\s]*(\d+)',
            r'(\d+)[_\s]*[_\s]*(\d+)',  # Two consecutive numbers
        ]
        
        for pattern in patterns:
            match = re.search(pattern, pdf_name, re.IGNORECASE)
            if match:
                try:
                    book_id = int(match.group(1))
                    chapter_id = int(match.group(2))
                    return book_id, chapter_id
                except (ValueError, IndexError):
                    continue
        
        return None, None
    
    def _find_matching_topic_file_by_metadata(self, pdf_path: str, topic_files: List[str]) -> Optional[str]:
        """Find matching topic file by checking metadata in topic files"""
        # Try to extract book_id and chapter_id from PDF filename
        pdf_name = os.path.basename(pdf_path)
        book_id, chapter_id = self._extract_book_chapter_from_pdf_name(pdf_name)
        
        if book_id is None or chapter_id is None:
            return None
        
        # Check each topic file's metadata
        for topic_path in topic_files:
            try:
                with open(topic_path, 'r', encoding='utf-8') as f:
                    topic_data = json.load(f)
                metadata = topic_data.get('metadata', {})
                topic_book_id = metadata.get('book_id')
                topic_chapter_id = metadata.get('chapter_id')
                
                if topic_book_id == book_id and topic_chapter_id == chapter_id:
                    return topic_path
            except Exception:
                continue
        
        return None
    
    def _add_ocr_pair_to_ui(self, pdf_path: str, topic_path: str):
        """Add a matched pair to the UI list"""
        pair_frame = ctk.CTkFrame(self.ocr_extraction_pairs_list_scroll)
        pair_frame.pack(fill="x", padx=5, pady=2)
        
        # PDF name
        pdf_name = os.path.basename(pdf_path)
        pdf_label = ctk.CTkLabel(pair_frame, text=f"PDF: {pdf_name}", 
                                font=ctk.CTkFont(size=10))
        pdf_label.pack(side="left", padx=5)
        
        # Topic file name
        topic_name = os.path.basename(topic_path)
        topic_label = ctk.CTkLabel(pair_frame, text=f"Topic: {topic_name}", 
                                  font=ctk.CTkFont(size=10))
        topic_label.pack(side="left", padx=5)
        
        # Status label
        status_label = ctk.CTkLabel(pair_frame, text="Pending", 
                                   text_color="gray", font=ctk.CTkFont(size=11))
        status_label.pack(side="right", padx=5)
        
        # Remove button
        remove_btn = ctk.CTkButton(pair_frame, text="X", width=30, height=20,
                                  command=lambda pf=pdf_path, tf=topic_path, w=pair_frame: 
                                  self.remove_ocr_extraction_pair(pf, tf, w))
        remove_btn.pack(side="right", padx=2)
        
        self.ocr_extraction_selected_pairs.append({
            'pdf_path': pdf_path,
            'topic_file_path': topic_path,
            'status_label': status_label,
            'frame': pair_frame,
            'output_path': None
        })
    
    def add_ocr_extraction_pair_manual(self):
        """Add a PDF + Topic file pair manually"""
        # Browse PDF
        pdf_path = filedialog.askopenfilename(
            title="Select PDF File",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")]
        )
        if not pdf_path:
            return
        
        # Browse Topic file
        topic_path = filedialog.askopenfilename(
            title="Select Pre-OCR Topic File (t*.json)",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        if not topic_path:
            return
        
        self._add_ocr_pair_to_ui(pdf_path, topic_path)
        self.logger.info(f"Added manual pair: PDF={os.path.basename(pdf_path)}, Topic={os.path.basename(topic_path)}")
    
    def remove_ocr_extraction_pair(self, pdf_path: str, topic_path: str, frame_widget):
        """Remove a pair from the selection list"""
        self.ocr_extraction_selected_pairs = [
            p for p in self.ocr_extraction_selected_pairs 
            if not (p['pdf_path'] == pdf_path and p['topic_file_path'] == topic_path)
        ]
        frame_widget.destroy()
    
    def _add_document_processing_file_to_ui(self, file_path: str):
        """Add a JSON file to the Document Processing UI list"""
        file_frame = ctk.CTkFrame(self.document_processing_files_list_scroll)
        file_frame.pack(fill="x", padx=5, pady=2)
        
        # File name
        file_name = os.path.basename(file_path)
        file_label = ctk.CTkLabel(file_frame, text=f"{file_name}", 
                                font=ctk.CTkFont(size=10))
        file_label.pack(side="left", padx=5)
        
        # Status label
        status_label = ctk.CTkLabel(file_frame, text="Pending", 
                                   text_color="gray", font=ctk.CTkFont(size=11))
        status_label.pack(side="right", padx=5)
        
        # Remove button
        remove_btn = ctk.CTkButton(file_frame, text="X", width=30, height=20,
                                  command=lambda fp=file_path, w=file_frame: 
                                  self.remove_document_processing_file(fp, w))
        remove_btn.pack(side="right", padx=2)
        
        # Store file info (if list doesn't exist, create it)
        if not hasattr(self, 'document_processing_file_info_list'):
            self.document_processing_file_info_list = []
        
        self.document_processing_file_info_list.append({
            'file_path': file_path,
            'status_label': status_label,
            'frame': file_frame,
            'output_path': None
        })
    
    def remove_document_processing_file(self, file_path: str, frame_widget):
        """Remove a file from the Document Processing selection list"""
        if hasattr(self, 'document_processing_selected_files'):
            self.document_processing_selected_files = [
                f for f in self.document_processing_selected_files if f != file_path
            ]
        if hasattr(self, 'document_processing_file_info_list'):
            self.document_processing_file_info_list = [
                f for f in self.document_processing_file_info_list if f['file_path'] != file_path
            ]
        frame_widget.destroy()
    
    def process_multiple_ocr_extractions(self):
        """Process multiple PDF + Topic file pairs for OCR Extraction"""
        def worker():
            try:
                if not self.ocr_extraction_selected_pairs:
                    self.root.after(0, lambda: messagebox.showwarning("Warning", "Please add at least one PDF + Topic pair"))
                    return
                
                # Disable button
                self.root.after(0, lambda: self.ocr_extraction_process_all_btn.configure(
                    state="disabled", text="Processing..."))
                
                # Get prompt
                prompt = self.ocr_extraction_prompt_text.get("1.0", tk.END).strip()
                if not prompt:
                    default_prompt = self.prompt_manager.get_prompt("OCR Extraction Prompt")
                    if default_prompt:
                        prompt = default_prompt
                        self.logger.info("Using default OCR Extraction prompt from prompts.json")
                    else:
                        self.root.after(0, lambda: messagebox.showerror("Error", "Please enter a prompt"))
                        return
                
                # Get delay
                try:
                    delay_seconds = int(self.ocr_extraction_delay_var.get())
                except ValueError:
                    delay_seconds = 5
                    self.logger.warning(f"Invalid delay value, using default: {delay_seconds} seconds")
                
                # Validate API keys
                if not self.api_key_manager.api_keys:
                    self.root.after(0, lambda: messagebox.showerror("Error", "Please load API keys first"))
                    return
                
                model_name = self.get_default_model()
                total_pairs = len(self.ocr_extraction_selected_pairs)
                completed = 0
                failed = 0
                
                # Reset progress bar
                self.root.after(0, lambda: self.ocr_extraction_progress_bar.set(0))
                
                # Process each pair
                for idx, pair_info in enumerate(self.ocr_extraction_selected_pairs):
                    pdf_path = pair_info['pdf_path']
                    topic_file_path = pair_info['topic_file_path']
                    status_label = pair_info['status_label']
                    pdf_name = os.path.basename(pdf_path)
                    topic_name = os.path.basename(topic_file_path)
                    
                    # Update status to processing
                    self.root.after(0, lambda sl=status_label: 
                                   sl.configure(text="Processing...", text_color="blue"))
                    
                    self.root.after(0, lambda idx=idx, total=total_pairs, pdf=pdf_name: 
                                   self.ocr_extraction_progress_label.configure(
                                       text=f"Processing pair {idx+1}/{total}: {pdf}"))
                    
                    # Progress bar
                    progress = idx / total_pairs
                    self.root.after(0, lambda p=progress: self.ocr_extraction_progress_bar.set(p))
                    
                    try:
                        # Progress callback for this pair
                        def progress_callback(msg: str):
                            self.root.after(0, lambda m=msg: 
                                           self.ocr_extraction_status_label.configure(text=m))
                        
                        # Process the pair
                        output_dir = self.get_default_output_dir(pdf_path)
                        final_output_path = self.multi_part_processor.process_ocr_extraction_with_topics(
                            pdf_path=pdf_path,
                            topic_file_path=topic_file_path,
                            base_prompt=prompt,
                            model_name=model_name,
                            temperature=0.7,
                            progress_callback=progress_callback,
                            output_dir=output_dir
                        )
                        
                        if final_output_path and os.path.exists(final_output_path):
                            pair_info['output_path'] = final_output_path
                            completed += 1
                            self.root.after(0, lambda sl=status_label, pdf=pdf_name, op=final_output_path: 
                                           sl.configure(text="Completed", text_color="green"))
                            self.logger.info(f"Completed: {pdf_name} + {topic_name} -> {final_output_path}")
                        else:
                            failed += 1
                            self.root.after(0, lambda sl=status_label, pdf=pdf_name: 
                                           sl.configure(text="Failed", text_color="red"))
                            self.logger.error(f"Failed: {pdf_name} + {topic_name}")
                    
                    except Exception as e:
                        failed += 1
                        self.root.after(0, lambda sl=status_label, pdf=pdf_name: 
                                       sl.configure(text="Error", text_color="red"))
                        self.logger.error(f"Error processing {pdf_name} + {topic_name}: {e}", exc_info=True)
                    
                    # Delay before next pair (except for the last one)
                    if idx < total_pairs - 1:
                        self.root.after(0, lambda d=delay_seconds: 
                                       self.ocr_extraction_progress_label.configure(
                                           text=f"Waiting {d} seconds before next pair..."))
                        time.sleep(delay_seconds)
                
                # Final update
                self.root.after(0, lambda: self.ocr_extraction_progress_bar.set(1.0))
                self.root.after(0, lambda c=completed, f=failed: 
                               self.ocr_extraction_progress_label.configure(
                                   text=f"Batch processing completed! {c} succeeded, {f} failed"))
                
                # Show summary
                summary = f"Batch Processing Complete!\n\n" \
                         f"Total: {total_pairs}\n" \
                         f"Completed: {completed}\n" \
                         f"Failed: {failed}"
                self.root.after(0, lambda s=summary: messagebox.showinfo("Batch Processing Complete", s))
                
            except Exception as e:
                error_msg = f"Batch processing error: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                self.root.after(0, lambda msg=error_msg: messagebox.showerror("Error", msg))
            finally:
                # Re-enable button
                self.root.after(0, lambda: self.ocr_extraction_process_all_btn.configure(
                    state="normal", text="Process All Pairs"))
        
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
        
        # Stage E File Selection - Batch Processing
        stage_e_frame = ctk.CTkFrame(main_frame)
        stage_e_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(
            stage_e_frame,
            text="Stage E JSON Files - Batch Processing",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=5)
        
        # File selection buttons
        buttons_frame = ctk.CTkFrame(stage_e_frame)
        buttons_frame.pack(fill="x", padx=10, pady=5)
        
        def browse_multiple_stage_e_files():
            filenames = filedialog.askopenfilenames(
                title="Select Stage E JSON files",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            if filenames:
                if not hasattr(self, 'stage_f_selected_files'):
                    self.stage_f_selected_files = []
                for filename in filenames:
                    if filename not in self.stage_f_selected_files:
                        self.stage_f_selected_files.append(filename)
                        self._add_stage_f_file_to_ui(filename)
        
        def select_folder_stage_e_files():
            folder_path = filedialog.askdirectory(
                title="Select folder containing Stage E JSON files"
            )
            if not folder_path:
                return
            
            import glob
            json_files = glob.glob(os.path.join(folder_path, "*.json"))
            json_files = [f for f in json_files if os.path.isfile(f)]
            
            if not json_files:
                messagebox.showinfo("Info", "No JSON files found in selected folder")
                return
            
            if not hasattr(self, 'stage_f_selected_files'):
                self.stage_f_selected_files = []
            
            added_count = 0
            for json_file in json_files:
                if json_file not in self.stage_f_selected_files:
                    self.stage_f_selected_files.append(json_file)
                    self._add_stage_f_file_to_ui(json_file)
                    added_count += 1
            
            messagebox.showinfo("Success", f"Added {added_count} JSON file(s) from folder")
        
        ctk.CTkButton(
            buttons_frame,
            text="Browse Multiple Files",
            command=browse_multiple_stage_e_files,
            width=180,
        ).pack(side="left", padx=5, pady=5)
        
        ctk.CTkButton(
            buttons_frame,
            text="Select Folder",
            command=select_folder_stage_e_files,
            width=180,
        ).pack(side="left", padx=5, pady=5)
        
        # Files list (scrollable)
        list_frame = ctk.CTkFrame(stage_e_frame)
        list_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            list_frame,
            text="Selected Files:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_f_files_list_scroll'):
            self.stage_f_files_list_scroll = ctk.CTkScrollableFrame(list_frame, height=150)
        self.stage_f_files_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize selected files list
        if not hasattr(self, 'stage_f_selected_files'):
            self.stage_f_selected_files = []
        
        if not hasattr(self, 'stage_f_file_info_list'):
            self.stage_f_file_info_list = []
        
        ctk.CTkLabel(
            stage_e_frame,
            text="Each JSON file should be a Stage E JSON file with first_image_point_id in metadata.",
            font=ctk.CTkFont(size=10),
            text_color="gray",
        ).pack(anchor="w", padx=15, pady=(0, 10))
        
        # Progress bar for batch processing
        if not hasattr(self, 'stage_f_progress_bar'):
            self.stage_f_progress_bar = ctk.CTkProgressBar(main_frame, width=400)
        self.stage_f_progress_bar.pack(pady=10)
        self.stage_f_progress_bar.set(0)
        
        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)
        
        self.stage_f_process_btn = ctk.CTkButton(
            process_btn_frame,
            text="Process Image File Catalog",
            command=self.process_stage_f_batch,
            width=200,
            height=40,
            font=ctk.CTkFont(size=16, weight="bold"),
            fg_color="blue"
        )
        self.stage_f_process_btn.pack(pady=10)
        
        # View CSV button
        self.stage_f_view_csv_btn = ctk.CTkButton(
            process_btn_frame,
            text="View CSV",
            command=self.view_csv_stage_f,
            width=150,
            height=40,
            font=ctk.CTkFont(size=14),
            fg_color="green",
            hover_color="darkgreen",
            state="disabled"
        )
        self.stage_f_view_csv_btn.pack(pady=10)
        
        # Status for Stage F
        self.stage_f_status_label = ctk.CTkLabel(main_frame, text="Ready", 
                                                 font=ctk.CTkFont(size=12), text_color="gray")
        self.stage_f_status_label.pack(pady=10)
    
    def _add_stage_f_file_to_ui(self, file_path: str):
        """Add a Stage E file to the UI list"""
        if not hasattr(self, 'stage_f_file_info_list'):
            self.stage_f_file_info_list = []
        
        file_frame = ctk.CTkFrame(self.stage_f_files_list_scroll)
        file_frame.pack(fill="x", padx=5, pady=2)
        
        file_name = os.path.basename(file_path)
        
        # File name label
        name_label = ctk.CTkLabel(
            file_frame,
            text=file_name,
            font=ctk.CTkFont(size=11),
            anchor="w"
        )
        name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
        
        # Status label
        status_label = ctk.CTkLabel(
            file_frame,
            text="Pending",
            font=ctk.CTkFont(size=10),
            text_color="gray",
            width=80
        )
        status_label.pack(side="right", padx=5, pady=5)
        
        # Remove button
        def remove_file():
            if file_path in self.stage_f_selected_files:
                self.stage_f_selected_files.remove(file_path)
            file_frame.destroy()
            # Remove from file_info_list
            self.stage_f_file_info_list = [
                info for info in self.stage_f_file_info_list 
                if info['file_path'] != file_path
            ]
        
        remove_btn = ctk.CTkButton(
            file_frame,
            text="✕",
            command=remove_file,
            width=30,
            height=25,
            fg_color="red",
            hover_color="darkred"
        )
        remove_btn.pack(side="right", padx=5, pady=5)
        
        # Store file info
        file_info = {
            'file_path': file_path,
            'status_label': status_label,
            'output_path': None
        }
        self.stage_f_file_info_list.append(file_info)
    
    def remove_stage_f_file(self, file_path: str, frame_widget):
        """Remove a file from the Stage F selection list"""
        if hasattr(self, 'stage_f_selected_files'):
            self.stage_f_selected_files = [
                f for f in self.stage_f_selected_files if f != file_path
            ]
        if hasattr(self, 'stage_f_file_info_list'):
            self.stage_f_file_info_list = [
                f for f in self.stage_f_file_info_list if f['file_path'] != file_path
            ]
        frame_widget.destroy()
    
    def process_stage_f_batch(self):
        """Process multiple Stage E files for Image File Catalog"""
        def worker():
            try:
                if not hasattr(self, 'stage_f_selected_files') or not self.stage_f_selected_files:
                    self.root.after(0, lambda: messagebox.showwarning(
                        "Warning", 
                        "Please add at least one Stage E JSON file"
                    ))
                    return
                
                self.stage_f_process_btn.configure(
                    state="disabled", 
                    text="Processing Batch..."
                )
                
                total_files = len(self.stage_f_selected_files)
                completed = 0
                failed = 0
                
                # Reset progress bar
                self.root.after(0, lambda: self.stage_f_progress_bar.set(0))
                
                # Process each file
                for idx, stage_e_path in enumerate(self.stage_f_selected_files):
                    file_name = os.path.basename(stage_e_path)
                    
                    # Update status to processing
                    if hasattr(self, 'stage_f_file_info_list'):
                        for file_info in self.stage_f_file_info_list:
                            if file_info['file_path'] == stage_e_path:
                                self.root.after(0, lambda sl=file_info['status_label']: 
                                               sl.configure(text="Processing...", text_color="blue"))
                                break
                    
                    # Update progress label
                    self.root.after(0, lambda idx=idx, total=total_files, fn=file_name: 
                                   self.stage_f_status_label.configure(
                                       text=f"Processing file {idx+1}/{total}: {fn}"))
                    
                    # Progress bar
                    progress = idx / total_files
                    self.root.after(0, lambda p=progress: self.stage_f_progress_bar.set(p))
                    
                    try:
                        def progress_callback(msg: str):
                            self.root.after(0, lambda m=msg: 
                                           self.stage_f_status_label.configure(text=m))
                        
                        # Process Stage F
                        output_path = self.stage_f_processor.process_stage_f(
                            stage_e_path=stage_e_path,
                            output_dir=self.get_default_output_dir(stage_e_path),
                            progress_callback=progress_callback
                        )
                        
                        if output_path and os.path.exists(output_path):
                            completed += 1
                            # Update status to completed
                            if hasattr(self, 'stage_f_file_info_list'):
                                for file_info in self.stage_f_file_info_list:
                                    if file_info['file_path'] == stage_e_path:
                                        file_info['output_path'] = output_path
                                        self.root.after(0, lambda sl=file_info['status_label']: 
                                                       sl.configure(text="Completed", text_color="green"))
                                        break
                            
                            self.logger.info(
                                f"Successfully processed: {file_name} -> {os.path.basename(output_path)}"
                            )
                        else:
                            failed += 1
                            # Update status to failed
                            if hasattr(self, 'stage_f_file_info_list'):
                                for file_info in self.stage_f_file_info_list:
                                    if file_info['file_path'] == stage_e_path:
                                        self.root.after(0, lambda sl=file_info['status_label']: 
                                                       sl.configure(text="Failed", text_color="red"))
                                        break
                            self.logger.error(f"Failed to process: {file_name}")
                            
                    except Exception as e:
                        failed += 1
                        self.logger.error(f"Error processing {file_name}: {str(e)}", exc_info=True)
                        # Update status to failed
                        if hasattr(self, 'stage_f_file_info_list'):
                            for file_info in self.stage_f_file_info_list:
                                if file_info['file_path'] == stage_e_path:
                                    self.root.after(0, lambda sl=file_info['status_label']: 
                                                   sl.configure(text="Failed", text_color="red"))
                                    break
                
                # Final progress update
                self.root.after(0, lambda: self.stage_f_progress_bar.set(1.0))
                
                self.root.after(0, lambda: self.stage_f_status_label.configure(
                    text=f"Batch completed: {completed} succeeded, {failed} failed",
                    text_color="green" if failed == 0 else "orange"
                ))
                
                # Show summary
                self.root.after(0, lambda: messagebox.showinfo(
                    "Batch Processing Complete",
                    f"Image File Catalog batch completed!\n\n"
                    f"Total files: {total_files}\n"
                    f"Successful: {completed}\n"
                    f"Failed: {failed}"
                ))
                
            except Exception as e:
                self.logger.error(f"Error in batch Stage F processing: {str(e)}", exc_info=True)
                self.root.after(0, lambda: messagebox.showerror(
                    "Error", 
                    f"Batch processing error:\n{str(e)}"
                ))
            finally:
                self.root.after(0, lambda: self.stage_f_process_btn.configure(
                    state="normal", 
                    text="Process Image File Catalog"
                ))
        
        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def process_stage_f(self):
        """Process Stage F in background thread (single file - kept for backward compatibility)"""
        def worker():
            try:
                self.stage_f_process_btn.configure(state="disabled", text="Processing...")
                self.update_stage_status("F", "processing")
                self.stage_f_status_label.configure(text="Processing Image File Catalog...", text_color="blue")
                
                # Validate inputs - check if batch mode files exist, otherwise use old single file mode
                if hasattr(self, 'stage_f_selected_files') and self.stage_f_selected_files:
                    # Use batch processing
                    self.process_stage_f_batch()
                    return
                
                # Old single file mode (backward compatibility)
                if hasattr(self, 'stage_f_stage_e_var'):
                    stage_e_path = self.stage_f_stage_e_var.get().strip()
                    
                    if not stage_e_path or not os.path.exists(stage_e_path):
                        messagebox.showerror("Error", "Please select a valid Stage E JSON file")
                        return
                    
                    def progress_callback(msg: str):
                        self.root.after(0, lambda: self.stage_f_status_label.configure(text=msg))
                    
                    # Process Stage F (use default output dir from main view)
                    output_path = self.stage_f_processor.process_stage_f(
                        stage_e_path=stage_e_path,
                        output_dir=self.get_default_output_dir(stage_e_path),
                        progress_callback=progress_callback
                    )
                    
                    if output_path:
                        self.last_stage_f_path = output_path
                        self.update_stage_status("F", "completed", output_path)
                        self.root.after(0, lambda: self.stage_f_view_csv_btn.configure(state="normal"))
                        self.stage_f_status_label.configure(
                            text=f"Stage F completed successfully!\nOutput: {os.path.basename(output_path)}",
                            text_color="green"
                        )
                        messagebox.showinfo("Success", f"Image File Catalog completed!\n\nOutput saved to:\n{output_path}")
                    else:
                        self.update_stage_status("F", "error")
                        self.stage_f_status_label.configure(text="Image File Catalog failed. Check logs for details.", text_color="red")
                        messagebox.showerror("Error", "Image File Catalog processing failed. Check logs for details.")
                else:
                    messagebox.showwarning("Warning", "Please add at least one Stage E JSON file")
            
            except Exception as e:
                self.logger.error(f"Error in Image File Catalog processing: {e}", exc_info=True)
                self.update_stage_status("F", "error")
                self.stage_f_status_label.configure(text=f"Error: {str(e)}", text_color="red")
                messagebox.showerror("Error", f"Image File Catalog processing error:\n{str(e)}")
            finally:
                self.root.after(0, lambda: self.stage_f_process_btn.configure(state="normal", text="Process Image File Catalog"))
        
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
        
        # Stage E Files Selection - Batch Processing
        stage_e_frame = ctk.CTkFrame(main_frame)
        stage_e_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(
            stage_e_frame,
            text="Stage E JSON Files - Batch Processing",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=5)
        
        # File selection buttons for Stage E
        buttons_frame_e = ctk.CTkFrame(stage_e_frame)
        buttons_frame_e.pack(fill="x", padx=10, pady=5)
        
        def browse_multiple_stage_e_files():
            filenames = filedialog.askopenfilenames(
                title="Select Stage E JSON files",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            if filenames:
                if not hasattr(self, 'stage_j_selected_stage_e_files'):
                    self.stage_j_selected_stage_e_files = []
                for filename in filenames:
                    if filename not in self.stage_j_selected_stage_e_files:
                        self.stage_j_selected_stage_e_files.append(filename)
                        self._add_stage_j_stage_e_file_to_ui(filename)
                self._update_stage_j_pairs()
        
        def select_folder_stage_e_files():
            folder_path = filedialog.askdirectory(
                title="Select folder containing Stage E JSON files"
            )
            if not folder_path:
                return
            
            import glob
            json_files = glob.glob(os.path.join(folder_path, "*.json"))
            json_files = [f for f in json_files if os.path.isfile(f)]
            
            if not json_files:
                messagebox.showinfo("Info", "No JSON files found in selected folder")
                return
            
            if not hasattr(self, 'stage_j_selected_stage_e_files'):
                self.stage_j_selected_stage_e_files = []
            
            added_count = 0
            for json_file in json_files:
                if json_file not in self.stage_j_selected_stage_e_files:
                    self.stage_j_selected_stage_e_files.append(json_file)
                    self._add_stage_j_stage_e_file_to_ui(json_file)
                    added_count += 1
            
            if added_count > 0:
                self._update_stage_j_pairs()
            messagebox.showinfo("Success", f"Added {added_count} JSON file(s) from folder")
        
        ctk.CTkButton(
            buttons_frame_e,
            text="Browse Multiple Stage E Files",
            command=browse_multiple_stage_e_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        ctk.CTkButton(
            buttons_frame_e,
            text="Select Folder (Stage E)",
            command=select_folder_stage_e_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        # Stage E files list (scrollable)
        list_frame_e = ctk.CTkFrame(stage_e_frame)
        list_frame_e.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            list_frame_e,
            text="Selected Stage E Files:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_j_stage_e_files_list_scroll'):
            self.stage_j_stage_e_files_list_scroll = ctk.CTkScrollableFrame(list_frame_e, height=100)
        self.stage_j_stage_e_files_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize selected files list
        if not hasattr(self, 'stage_j_selected_stage_e_files'):
            self.stage_j_selected_stage_e_files = []
        
        # Word Files Selection - Batch Processing
        word_frame = ctk.CTkFrame(main_frame)
        word_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(
            word_frame,
            text="Word Files (Test Questions) - Batch Processing",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=5)
        
        # File selection buttons for Word files
        buttons_frame_word = ctk.CTkFrame(word_frame)
        buttons_frame_word.pack(fill="x", padx=10, pady=5)
        
        def browse_multiple_word_files():
            filenames = filedialog.askopenfilenames(
                title="Select Word files",
                filetypes=[("Word Documents", "*.docx *.doc"), ("All files", "*.*")]
            )
            if filenames:
                if not hasattr(self, 'stage_j_selected_word_files'):
                    self.stage_j_selected_word_files = []
                for filename in filenames:
                    if filename not in self.stage_j_selected_word_files:
                        self.stage_j_selected_word_files.append(filename)
                        self._add_stage_j_word_file_to_ui(filename)
                self._update_stage_j_pairs()
        
        def select_folder_word_files():
            folder_path = filedialog.askdirectory(
                title="Select folder containing Word files"
            )
            if not folder_path:
                return
            
            import glob
            word_files = glob.glob(os.path.join(folder_path, "*.docx")) + glob.glob(os.path.join(folder_path, "*.doc"))
            word_files = [f for f in word_files if os.path.isfile(f)]
            
            if not word_files:
                messagebox.showinfo("Info", "No Word files found in selected folder")
                return
            
            if not hasattr(self, 'stage_j_selected_word_files'):
                self.stage_j_selected_word_files = []
            
            added_count = 0
            for word_file in word_files:
                if word_file not in self.stage_j_selected_word_files:
                    self.stage_j_selected_word_files.append(word_file)
                    self._add_stage_j_word_file_to_ui(word_file)
                    added_count += 1
            
            if added_count > 0:
                self._update_stage_j_pairs()
            messagebox.showinfo("Success", f"Added {added_count} Word file(s) from folder")
        
        ctk.CTkButton(
            buttons_frame_word,
            text="Browse Multiple Word Files",
            command=browse_multiple_word_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        ctk.CTkButton(
            buttons_frame_word,
            text="Select Folder (Word)",
            command=select_folder_word_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        # Word files list (scrollable)
        list_frame_word = ctk.CTkFrame(word_frame)
        list_frame_word.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            list_frame_word,
            text="Selected Word Files:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_j_word_files_list_scroll'):
            self.stage_j_word_files_list_scroll = ctk.CTkScrollableFrame(list_frame_word, height=100)
        self.stage_j_word_files_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize selected files list
        if not hasattr(self, 'stage_j_selected_word_files'):
            self.stage_j_selected_word_files = []
        
        # Pairs Section
        pairs_frame = ctk.CTkFrame(main_frame)
        pairs_frame.pack(fill="x", pady=10)
        
        pairs_header_frame = ctk.CTkFrame(pairs_frame)
        pairs_header_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(
            pairs_header_frame,
            text="Pairs (Stage E ↔ Word File):",
            font=ctk.CTkFont(size=14, weight="bold"),
        ).pack(side="left", padx=10, pady=5)
        
        ctk.CTkButton(
            pairs_header_frame,
            text="Auto-Pair",
            command=self._auto_pair_stage_j_files,
            width=120,
            height=30,
            fg_color="green",
            hover_color="darkgreen"
        ).pack(side="right", padx=10, pady=5)
        
        # Pairs list (scrollable)
        pairs_list_frame = ctk.CTkFrame(pairs_frame)
        pairs_list_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        if not hasattr(self, 'stage_j_pairs_list_scroll'):
            self.stage_j_pairs_list_scroll = ctk.CTkScrollableFrame(pairs_list_frame, height=200)
        self.stage_j_pairs_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize pairs list
        if not hasattr(self, 'stage_j_pairs'):
            self.stage_j_pairs = []
        
        if not hasattr(self, 'stage_j_pairs_info_list'):
            self.stage_j_pairs_info_list = []
        
        # Image File Catalog Selection (Optional)
        stage_f_frame = ctk.CTkFrame(main_frame)
        stage_f_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(stage_f_frame, text="Image File Catalog JSON (Optional - f.json):", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        # Stage F is now auto-detected per pair, so we don't need a single file selection anymore
        # This section is kept for backward compatibility but not used in batch mode
        
        # Prompt Section
        prompt_frame = ctk.CTkFrame(main_frame)
        prompt_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(
            prompt_frame,
            text="Prompt for Imp & Type Generation:",
            font=ctk.CTkFont(size=14, weight="bold"),
        ).pack(anchor="w", padx=10, pady=5)

        # Stage J prompt mode (default vs custom)
        stage_j_mode_frame = ctk.CTkFrame(prompt_frame)
        stage_j_mode_frame.pack(fill="x", padx=10, pady=(0, 5))
        if not hasattr(self, 'stage_j_prompt_type_var'):
            self.stage_j_prompt_type_var = ctk.StringVar(value="default")
        ctk.CTkRadioButton(
            stage_j_mode_frame,
            text="Use Default Prompt",
            variable=self.stage_j_prompt_type_var,
            value="default",
            command=self.on_stage_j_prompt_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)
        ctk.CTkRadioButton(
            stage_j_mode_frame,
            text="Use Custom Prompt",
            variable=self.stage_j_prompt_type_var,
            value="custom",
            command=self.on_stage_j_prompt_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)

        # Default Stage J prompt combobox (predefined prompts)
        stage_j_default_frame = ctk.CTkFrame(prompt_frame)
        stage_j_default_frame.pack(fill="x", padx=10, pady=(0, 10))

        ctk.CTkLabel(
            stage_j_default_frame,
            text="Default Stage J Prompt:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", pady=(0, 5))

        stage_j_prompt_names = self.prompt_manager.get_prompt_names()
        # We will later set a specific preferred name (e.g., \"Stage J - Importance & Type Prompt\")
        # For now, fall back to the first available prompt.
        preferred_stage_j_name = "Importance & Type Prompt"
        if preferred_stage_j_name in stage_j_prompt_names:
            stage_j_default_value = preferred_stage_j_name
        else:
            stage_j_default_value = stage_j_prompt_names[0] if stage_j_prompt_names else ""
        self.stage_j_default_prompt_var = ctk.StringVar(value=stage_j_default_value)
        self.stage_j_default_prompt_combo = ctk.CTkComboBox(
            stage_j_default_frame,
            values=stage_j_prompt_names,
            variable=self.stage_j_default_prompt_var,
            width=400,
            command=self.on_stage_j_default_prompt_selected,
        )
        self.stage_j_default_prompt_combo.pack(anchor="w", pady=(0, 5))

        # Textbox for Stage J prompt (default-filled or custom)
        if not hasattr(self, 'stage_j_prompt_text'):
            self.stage_j_prompt_text = ctk.CTkTextbox(prompt_frame, height=150, font=self.farsi_text_font)
        else:
            try:
                self.stage_j_prompt_text.pack_forget()
            except Exception:
                pass
        self.stage_j_prompt_text.pack(fill="x", padx=10, pady=5)

        # Apply initial default/custom state and fill textbox
        self.on_stage_j_prompt_type_change()
        
        # Model Selection
        model_frame = ctk.CTkFrame(main_frame)
        model_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(model_frame, text="Model:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        # Get default model from main model selection if available
        default_model = "gemini-2.5-pro"
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
                text="Process Importance & Type Tagging",
                command=self.process_stage_j_batch,
                width=200,
                height=40,
                font=ctk.CTkFont(size=16, weight="bold"),
                fg_color="blue"
            )
        self.stage_j_process_btn.pack(pady=10)
        
        # View CSV button
        if not hasattr(self, 'stage_j_view_csv_btn'):
            self.stage_j_view_csv_btn = ctk.CTkButton(
                process_btn_frame,
                text="View CSV",
                command=self.view_csv_stage_j,
                width=150,
                height=40,
                font=ctk.CTkFont(size=14),
                fg_color="green",
                hover_color="darkgreen",
                state="disabled"
            )
        self.stage_j_view_csv_btn.pack(pady=10)
        
        # Status for Stage J
        if not hasattr(self, 'stage_j_status_label'):
            self.stage_j_status_label = ctk.CTkLabel(main_frame, text="Ready", 
                                                     font=ctk.CTkFont(size=12), text_color="gray")
        self.stage_j_status_label.pack(pady=10)
        
        # Progress bar for batch processing
        if not hasattr(self, 'stage_j_progress_bar'):
            self.stage_j_progress_bar = ctk.CTkProgressBar(main_frame, width=400)
        self.stage_j_progress_bar.pack(pady=10)
        self.stage_j_progress_bar.set(0)
    
    def _add_stage_j_stage_e_file_to_ui(self, file_path: str):
        """Add a Stage E file to the UI list"""
        file_frame = ctk.CTkFrame(self.stage_j_stage_e_files_list_scroll)
        file_frame.pack(fill="x", padx=5, pady=2)
        
        file_name = os.path.basename(file_path)
        
        name_label = ctk.CTkLabel(
            file_frame,
            text=file_name,
            font=ctk.CTkFont(size=11),
            anchor="w"
        )
        name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
        
        def remove_file():
            if file_path in self.stage_j_selected_stage_e_files:
                self.stage_j_selected_stage_e_files.remove(file_path)
            file_frame.destroy()
            self._update_stage_j_pairs()
        
        remove_btn = ctk.CTkButton(
            file_frame,
            text="✕",
            command=remove_file,
            width=30,
            height=25,
            fg_color="red",
            hover_color="darkred"
        )
        remove_btn.pack(side="right", padx=5, pady=5)
    
    def _add_stage_j_word_file_to_ui(self, file_path: str):
        """Add a Word file to the UI list"""
        file_frame = ctk.CTkFrame(self.stage_j_word_files_list_scroll)
        file_frame.pack(fill="x", padx=5, pady=2)
        
        file_name = os.path.basename(file_path)
        
        name_label = ctk.CTkLabel(
            file_frame,
            text=file_name,
            font=ctk.CTkFont(size=11),
            anchor="w"
        )
        name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
        
        def remove_file():
            if file_path in self.stage_j_selected_word_files:
                self.stage_j_selected_word_files.remove(file_path)
            file_frame.destroy()
            self._update_stage_j_pairs()
        
        remove_btn = ctk.CTkButton(
            file_frame,
            text="✕",
            command=remove_file,
            width=30,
            height=25,
            fg_color="red",
            hover_color="darkred"
        )
        remove_btn.pack(side="right", padx=5, pady=5)
    
    def _extract_book_chapter_from_stage_e(self, stage_e_path: str):
        """Extract book and chapter from Stage E file (from PointId or filename)"""
        try:
            # Try to load Stage E and extract from PointId
            data = json.load(open(stage_e_path, 'r', encoding='utf-8'))
            records = data.get("data") or data.get("rows", [])
            if records and records[0].get("PointId"):
                point_id = records[0].get("PointId")
                if isinstance(point_id, str) and len(point_id) >= 6:
                    book_id = int(point_id[0:3])
                    chapter_id = int(point_id[3:6])
                    return book_id, chapter_id
        except:
            pass
        
        # Fallback: try to extract from filename (e{book}{chapter}.json)
        try:
            basename = os.path.basename(stage_e_path)
            name_without_ext = os.path.splitext(basename)[0]
            if name_without_ext.startswith('e') and len(name_without_ext) >= 7:
                book_chapter = name_without_ext[1:]
                book_id = int(book_chapter[0:3])
                chapter_id = int(book_chapter[3:6])
                return book_id, chapter_id
        except:
            pass
        
        return None, None
    
    def _extract_book_chapter_from_word_filename(self, word_path: str):
        """Extract book and chapter from Word filename (various patterns)"""
        import re
        basename = os.path.basename(word_path)
        name_without_ext = os.path.splitext(basename)[0]
        
        # Try various patterns
        patterns = [
            r'ch(\d{3})_(\d{3})',  # ch105_003.docx
            r'chapter_(\d{3})_(\d{3})',  # chapter_105_003.docx
            r'(\d{3})_(\d{3})',  # 105_003.docx
            r'book(\d{3})_chapter(\d{3})',  # book105_chapter003.docx
            r'e(\d{3})(\d{3})',  # e105003.docx
        ]
        
        for pattern in patterns:
            match = re.search(pattern, name_without_ext, re.IGNORECASE)
            if match:
                book_id = int(match.group(1))
                chapter_id = int(match.group(2))
                return book_id, chapter_id
        
        return None, None
    
    def _auto_pair_stage_j_files(self):
        """Auto-pair Stage E files with Word files based on Book/Chapter"""
        if not hasattr(self, 'stage_j_selected_stage_e_files') or not self.stage_j_selected_stage_e_files:
            messagebox.showwarning("Warning", "Please add at least one Stage E file")
            return
        
        if not hasattr(self, 'stage_j_selected_word_files') or not self.stage_j_selected_word_files:
            messagebox.showwarning("Warning", "Please add at least one Word file")
            return
        
        pairs = []
        paired_word_files = set()
        
        for stage_e_path in self.stage_j_selected_stage_e_files:
            book_id, chapter_id = self._extract_book_chapter_from_stage_e(stage_e_path)
            
            if book_id is None or chapter_id is None:
                # Can't extract book/chapter, skip
                continue
            
            # Find matching Word file
            matched_word = None
            for word_path in self.stage_j_selected_word_files:
                if word_path in paired_word_files:
                    continue
                
                word_book, word_chapter = self._extract_book_chapter_from_word_filename(word_path)
                
                if word_book == book_id and word_chapter == chapter_id:
                    matched_word = word_path
                    paired_word_files.add(word_path)
                    break
            
            # Find Stage F file (auto-detect)
            stage_e_dir = os.path.dirname(stage_e_path)
            stage_e_basename = os.path.basename(stage_e_path)
            stage_e_name_without_ext = os.path.splitext(stage_e_basename)[0]
            stage_f_path = os.path.join(stage_e_dir, f"f_{stage_e_name_without_ext}.json")
            if not os.path.exists(stage_f_path):
                stage_f_path = os.path.join(stage_e_dir, "f.json")
            if not os.path.exists(stage_f_path):
                stage_f_path = None
            
            pair = {
                'stage_e_path': stage_e_path,
                'word_path': matched_word,
                'stage_f_path': stage_f_path,
                'status': 'pending',
                'output_path': None,
                'error': None
            }
            pairs.append(pair)
        
        self.stage_j_pairs = pairs
        self._update_stage_j_pairs_ui()
        
        paired_count = sum(1 for p in pairs if p['word_path'] is not None)
        unpaired_count = len(pairs) - paired_count
        
        messagebox.showinfo(
            "Auto-Pairing Complete",
            f"Paired: {paired_count}\nUnpaired: {unpaired_count}"
        )
    
    def _update_stage_j_pairs(self):
        """Update pairs when files are added/removed"""
        # Clear existing pairs UI
        for widget in self.stage_j_pairs_list_scroll.winfo_children():
            widget.destroy()
        
        self.stage_j_pairs_info_list = []
        
        # Re-pair if we have pairs
        if hasattr(self, 'stage_j_pairs') and self.stage_j_pairs:
            self._update_stage_j_pairs_ui()
    
    def _update_stage_j_pairs_ui(self):
        """Update the pairs UI display"""
        # Clear existing pairs UI
        for widget in self.stage_j_pairs_list_scroll.winfo_children():
            widget.destroy()
        
        self.stage_j_pairs_info_list = []
        
        if not hasattr(self, 'stage_j_pairs') or not self.stage_j_pairs:
            return
        
        for idx, pair in enumerate(self.stage_j_pairs):
            pair_frame = ctk.CTkFrame(self.stage_j_pairs_list_scroll)
            pair_frame.pack(fill="x", padx=5, pady=2)
            
            # Stage E file name
            stage_e_name = os.path.basename(pair['stage_e_path'])
            stage_e_label = ctk.CTkLabel(
                pair_frame,
                text=stage_e_name,
                font=ctk.CTkFont(size=10),
                width=150
            )
            stage_e_label.pack(side="left", padx=5, pady=5)
            
            # Arrow
            ctk.CTkLabel(pair_frame, text="↔", font=ctk.CTkFont(size=14)).pack(side="left", padx=5)
            
            # Word file name or dropdown
            word_frame = ctk.CTkFrame(pair_frame)
            word_frame.pack(side="left", padx=5, pady=5, fill="x", expand=True)
            
            if pair['word_path']:
                word_name = os.path.basename(pair['word_path'])
                word_label = ctk.CTkLabel(
                    word_frame,
                    text=word_name,
                    font=ctk.CTkFont(size=10),
                    text_color="green"
                )
                word_label.pack(side="left", padx=5)
            else:
                word_label = ctk.CTkLabel(
                    word_frame,
                    text="(Not Paired)",
                    font=ctk.CTkFont(size=10),
                    text_color="orange"
                )
                word_label.pack(side="left", padx=5)
                
                # Dropdown for manual pairing
                if hasattr(self, 'stage_j_selected_word_files') and self.stage_j_selected_word_files:
                    word_var = ctk.StringVar(value="Select Word file...")
                    word_combo = ctk.CTkComboBox(
                        word_frame,
                        values=["Select Word file..."] + [os.path.basename(f) for f in self.stage_j_selected_word_files],
                        variable=word_var,
                        width=200,
                        command=lambda val, p=pair, idx=idx: self._manual_pair_word(p, idx, val)
                    )
                    word_combo.pack(side="left", padx=5)
            
            # Status label
            status_label = ctk.CTkLabel(
                pair_frame,
                text="Pending",
                font=ctk.CTkFont(size=10),
                text_color="gray",
                width=80
            )
            status_label.pack(side="right", padx=5, pady=5)
            
            # Remove button
            def remove_pair(p=pair):
                if p in self.stage_j_pairs:
                    self.stage_j_pairs.remove(p)
                self._update_stage_j_pairs_ui()
            
            remove_btn = ctk.CTkButton(
                pair_frame,
                text="✕",
                command=remove_pair,
                width=30,
                height=25,
                fg_color="red",
                hover_color="darkred"
            )
            remove_btn.pack(side="right", padx=5, pady=5)
            
            # Store pair info
            pair_info = {
                'pair': pair,
                'status_label': status_label,
                'frame': pair_frame
            }
            self.stage_j_pairs_info_list.append(pair_info)
    
    def _manual_pair_word(self, pair, pair_idx, selected_value):
        """Manually pair a Word file with a Stage E file"""
        if selected_value == "Select Word file..." or not selected_value:
            return
        
        # Find the Word file path
        word_path = None
        for word_file in self.stage_j_selected_word_files:
            if os.path.basename(word_file) == selected_value:
                word_path = word_file
                break
        
        if word_path:
            pair['word_path'] = word_path
            self._update_stage_j_pairs_ui()
    
    def validate_stage_j_files(self):
        """Validate Stage J input files (kept for backward compatibility, not used in batch mode)"""
        # Validation is now done per pair in batch mode
        pass
    
    def process_stage_j_batch(self):
        """Process multiple Stage E + Word file pairs for Stage J"""
        def worker():
            try:
                if not hasattr(self, 'stage_j_pairs') or not self.stage_j_pairs:
                    self.root.after(0, lambda: messagebox.showwarning(
                        "Warning",
                        "Please add files and create pairs first. Click 'Auto-Pair' button."
                    ))
                    return
                
                # Filter pairs that have both Stage E and Word file
                valid_pairs = [p for p in self.stage_j_pairs if p['stage_e_path'] and p['word_path']]
                
                if not valid_pairs:
                    self.root.after(0, lambda: messagebox.showwarning(
                        "Warning",
                        "No valid pairs found. Each pair must have both Stage E and Word file."
                    ))
                    return
                
                self.stage_j_process_btn.configure(
                    state="disabled",
                    text="Processing Batch..."
                )
                
                # Get prompt
                prompt = self.stage_j_prompt_text.get("1.0", tk.END).strip()
                if not prompt:
                    default_prompt = self.prompt_manager.get_prompt("Importance & Type Prompt")
                    if default_prompt:
                        prompt = default_prompt
                        self.logger.info("Using default Importance & Type prompt from prompts.json")
                    else:
                        self.root.after(0, lambda: messagebox.showerror("Error", "Please enter a prompt"))
                        return
                
                # Get model
                model_name = self.stage_j_model_var.get() if hasattr(self, 'stage_j_model_var') else "gemini-2.5-pro"
                
                total_pairs = len(valid_pairs)
                completed = 0
                failed = 0
                
                # Reset progress bar
                self.root.after(0, lambda: self.stage_j_progress_bar.set(0))
                
                # Process each pair
                for idx, pair in enumerate(valid_pairs):
                    stage_e_path = pair['stage_e_path']
                    word_path = pair['word_path']
                    stage_f_path = pair.get('stage_f_path')
                    
                    stage_e_name = os.path.basename(stage_e_path)
                    word_name = os.path.basename(word_path)
                    
                    # Update status to processing
                    if hasattr(self, 'stage_j_pairs_info_list'):
                        for pair_info in self.stage_j_pairs_info_list:
                            if pair_info['pair'] == pair:
                                self.root.after(0, lambda sl=pair_info['status_label']:
                                               sl.configure(text="Processing...", text_color="blue"))
                                break
                    
                    # Update progress label
                    self.root.after(0, lambda idx=idx, total=total_pairs, se=stage_e_name, w=word_name:
                                   self.stage_j_status_label.configure(
                                       text=f"Processing pair {idx+1}/{total}: {se} ↔ {w}"))
                    
                    # Progress bar
                    progress = idx / total_pairs
                    self.root.after(0, lambda p=progress: self.stage_j_progress_bar.set(p))
                    
                    try:
                        def progress_callback(msg: str):
                            self.root.after(0, lambda m=msg:
                                           self.stage_j_status_label.configure(text=m))
                        
                        # Process Stage J
                        output_path = self.stage_j_processor.process_stage_j(
                            stage_e_path=stage_e_path,
                            word_file_path=word_path,
                            stage_f_path=stage_f_path,
                            prompt=prompt,
                            model_name=model_name,
                            output_dir=self.get_default_output_dir(stage_e_path),
                            progress_callback=progress_callback
                        )
                        
                        if output_path and os.path.exists(output_path):
                            completed += 1
                            pair['output_path'] = output_path
                            pair['status'] = 'completed'
                            
                            # Update status to completed
                            if hasattr(self, 'stage_j_pairs_info_list'):
                                for pair_info in self.stage_j_pairs_info_list:
                                    if pair_info['pair'] == pair:
                                        self.root.after(0, lambda sl=pair_info['status_label']:
                                                       sl.configure(text="Completed", text_color="green"))
                                        break
                            
                            self.logger.info(
                                f"Successfully processed: {stage_e_name} ↔ {word_name} -> {os.path.basename(output_path)}"
                            )
                        else:
                            failed += 1
                            pair['status'] = 'failed'
                            pair['error'] = "Processing returned no output"
                            
                            # Update status to failed
                            if hasattr(self, 'stage_j_pairs_info_list'):
                                for pair_info in self.stage_j_pairs_info_list:
                                    if pair_info['pair'] == pair:
                                        self.root.after(0, lambda sl=pair_info['status_label']:
                                                       sl.configure(text="Failed", text_color="red"))
                                        break
                            
                            self.logger.error(f"Failed to process: {stage_e_name} ↔ {word_name}")
                            
                    except Exception as e:
                        failed += 1
                        pair['status'] = 'failed'
                        pair['error'] = str(e)
                        self.logger.error(f"Error processing {stage_e_name} ↔ {word_name}: {str(e)}", exc_info=True)
                        
                        # Update status to failed
                        if hasattr(self, 'stage_j_pairs_info_list'):
                            for pair_info in self.stage_j_pairs_info_list:
                                if pair_info['pair'] == pair:
                                    self.root.after(0, lambda sl=pair_info['status_label']:
                                                   sl.configure(text="Failed", text_color="red"))
                                    break
                
                # Final progress update
                self.root.after(0, lambda: self.stage_j_progress_bar.set(1.0))
                
                self.root.after(0, lambda: self.stage_j_status_label.configure(
                    text=f"Batch completed: {completed} succeeded, {failed} failed",
                    text_color="green" if failed == 0 else "orange"
                ))
                
                # Show summary
                self.root.after(0, lambda: messagebox.showinfo(
                    "Batch Processing Complete",
                    f"Stage J batch completed!\n\n"
                    f"Total pairs: {total_pairs}\n"
                    f"Successful: {completed}\n"
                    f"Failed: {failed}"
                ))
                
            except Exception as e:
                self.logger.error(f"Error in batch Stage J processing: {str(e)}", exc_info=True)
                self.root.after(0, lambda: messagebox.showerror(
                    "Error",
                    f"Batch processing error:\n{str(e)}"
                ))
            finally:
                self.root.after(0, lambda: self.stage_j_process_btn.configure(
                    state="normal",
                    text="Process Importance & Type Tagging"
                ))
        
        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def process_stage_j(self):
        """Process Stage J (redirects to batch processing)"""
        # Redirect to batch processing
        self.process_stage_j_batch()
    
    def _add_stage_h_stage_j_file_to_ui(self, file_path: str):
        """Add a Stage J file to the UI list"""
        file_frame = ctk.CTkFrame(self.stage_h_stage_j_files_list_scroll)
        file_frame.pack(fill="x", padx=5, pady=2)
        
        file_name = os.path.basename(file_path)
        
        name_label = ctk.CTkLabel(
            file_frame,
            text=file_name,
            font=ctk.CTkFont(size=11),
            anchor="w"
        )
        name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
        
        def remove_file():
            if file_path in self.stage_h_selected_stage_j_files:
                self.stage_h_selected_stage_j_files.remove(file_path)
            file_frame.destroy()
            self._update_stage_h_pairs()
        
        remove_btn = ctk.CTkButton(
            file_frame,
            text="✕",
            command=remove_file,
            width=30,
            height=25,
            fg_color="red",
            hover_color="darkred"
        )
        remove_btn.pack(side="right", padx=5, pady=5)
    
    def _add_stage_h_stage_f_file_to_ui(self, file_path: str):
        """Add a Stage F file to the UI list"""
        file_frame = ctk.CTkFrame(self.stage_h_stage_f_files_list_scroll)
        file_frame.pack(fill="x", padx=5, pady=2)
        
        file_name = os.path.basename(file_path)
        
        name_label = ctk.CTkLabel(
            file_frame,
            text=file_name,
            font=ctk.CTkFont(size=11),
            anchor="w"
        )
        name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
        
        def remove_file():
            if file_path in self.stage_h_selected_stage_f_files:
                self.stage_h_selected_stage_f_files.remove(file_path)
            file_frame.destroy()
            self._update_stage_h_pairs()
        
        remove_btn = ctk.CTkButton(
            file_frame,
            text="✕",
            command=remove_file,
            width=30,
            height=25,
            fg_color="red",
            hover_color="darkred"
        )
        remove_btn.pack(side="right", padx=5, pady=5)
    
    def _extract_book_chapter_from_stage_j(self, stage_j_path: str):
        """Extract book and chapter from Stage J file (from PointId or filename)"""
        try:
            # Try to load Stage J and extract from PointId
            data = json.load(open(stage_j_path, 'r', encoding='utf-8'))
            records = data.get("data") or data.get("rows", [])
            if records and records[0].get("PointId"):
                point_id = records[0].get("PointId")
                if isinstance(point_id, str) and len(point_id) >= 6:
                    book_id = int(point_id[0:3])
                    chapter_id = int(point_id[3:6])
                    return book_id, chapter_id
        except:
            pass
        
        # Fallback: try to extract from filename (a{book}{chapter}.json)
        try:
            basename = os.path.basename(stage_j_path)
            name_without_ext = os.path.splitext(basename)[0]
            if name_without_ext.startswith('a') and len(name_without_ext) >= 7:
                book_chapter = name_without_ext[1:]
                book_id = int(book_chapter[0:3])
                chapter_id = int(book_chapter[3:6])
                return book_id, chapter_id
        except:
            pass
        
        return None, None
    
    def _extract_book_chapter_from_stage_f_filename(self, stage_f_path: str):
        """Extract book and chapter from Stage F filename"""
        try:
            basename = os.path.basename(stage_f_path)
            name_without_ext = os.path.splitext(basename)[0]
            
            # Try pattern: f_e{book}{chapter}.json
            if name_without_ext.startswith('f_e') and len(name_without_ext) >= 9:
                book_chapter = name_without_ext[3:]  # Remove 'f_e' prefix
                book_id = int(book_chapter[0:3])
                chapter_id = int(book_chapter[3:6])
                return book_id, chapter_id
            
            # Try pattern: f_{book}{chapter}.json
            if name_without_ext.startswith('f_') and len(name_without_ext) >= 8:
                book_chapter = name_without_ext[2:]  # Remove 'f_' prefix
                if len(book_chapter) >= 6:
                    book_id = int(book_chapter[0:3])
                    chapter_id = int(book_chapter[3:6])
                    return book_id, chapter_id
        except:
            pass
        
        return None, None
    
    def _auto_pair_stage_h_files(self):
        """Auto-pair Stage J files with Stage F files based on Book/Chapter"""
        if not hasattr(self, 'stage_h_selected_stage_j_files') or not self.stage_h_selected_stage_j_files:
            messagebox.showwarning("Warning", "Please add at least one Stage J file")
            return
        
        if not hasattr(self, 'stage_h_selected_stage_f_files') or not self.stage_h_selected_stage_f_files:
            messagebox.showwarning("Warning", "Please add at least one Stage F file")
            return
        
        pairs = []
        paired_stage_f_files = set()
        
        for stage_j_path in self.stage_h_selected_stage_j_files:
            book_id, chapter_id = self._extract_book_chapter_from_stage_j(stage_j_path)
            
            if book_id is None or chapter_id is None:
                # Can't extract book/chapter, try to find Stage F in same directory
                stage_j_dir = os.path.dirname(stage_j_path)
                stage_j_basename = os.path.basename(stage_j_path)
                stage_j_name_without_ext = os.path.splitext(stage_j_basename)[0]
                
                # Try to construct Stage F filename
                if stage_j_name_without_ext.startswith('a') and len(stage_j_name_without_ext) >= 7:
                    book_chapter = stage_j_name_without_ext[1:]
                    stage_e_name = f"e{book_chapter}"
                    stage_f_path = os.path.join(stage_j_dir, f"f_{stage_e_name}.json")
                    if not os.path.exists(stage_f_path):
                        stage_f_path = os.path.join(stage_j_dir, "f.json")
                else:
                    stage_f_path = os.path.join(stage_j_dir, "f.json")
                
                if os.path.exists(stage_f_path) and stage_f_path in self.stage_h_selected_stage_f_files:
                    matched_stage_f = stage_f_path
                    paired_stage_f_files.add(stage_f_path)
                else:
                    matched_stage_f = None
            else:
                # Find matching Stage F file
                matched_stage_f = None
                for stage_f_path in self.stage_h_selected_stage_f_files:
                    if stage_f_path in paired_stage_f_files:
                        continue
                    
                    stage_f_book, stage_f_chapter = self._extract_book_chapter_from_stage_f_filename(stage_f_path)
                    
                    if stage_f_book == book_id and stage_f_chapter == chapter_id:
                        matched_stage_f = stage_f_path
                        paired_stage_f_files.add(stage_f_path)
                        break
                
                # If no match found, try to find Stage F in same directory
                if matched_stage_f is None:
                    stage_j_dir = os.path.dirname(stage_j_path)
                    stage_e_name = f"e{book_id:03d}{chapter_id:03d}"
                    stage_f_path = os.path.join(stage_j_dir, f"f_{stage_e_name}.json")
                    if not os.path.exists(stage_f_path):
                        stage_f_path = os.path.join(stage_j_dir, "f.json")
                    if os.path.exists(stage_f_path) and stage_f_path in self.stage_h_selected_stage_f_files:
                        matched_stage_f = stage_f_path
                        paired_stage_f_files.add(stage_f_path)
            
            pair = {
                'stage_j_path': stage_j_path,
                'stage_f_path': matched_stage_f,
                'status': 'pending',
                'output_path': None,
                'error': None
            }
            pairs.append(pair)
        
        self.stage_h_pairs = pairs
        self._update_stage_h_pairs_ui()
        
        paired_count = sum(1 for p in pairs if p['stage_f_path'] is not None)
        unpaired_count = len(pairs) - paired_count
        
        messagebox.showinfo(
            "Auto-Pairing Complete",
            f"Paired: {paired_count}\nUnpaired: {unpaired_count}"
        )
    
    def _update_stage_h_pairs(self):
        """Update pairs when files are added/removed"""
        # Clear existing pairs UI
        for widget in self.stage_h_pairs_list_scroll.winfo_children():
            widget.destroy()
        
        self.stage_h_pairs_info_list = []
        
        # Re-pair if we have pairs
        if hasattr(self, 'stage_h_pairs') and self.stage_h_pairs:
            self._update_stage_h_pairs_ui()
    
    def _update_stage_h_pairs_ui(self):
        """Update the pairs UI display"""
        # Clear existing pairs UI
        for widget in self.stage_h_pairs_list_scroll.winfo_children():
            widget.destroy()
        
        self.stage_h_pairs_info_list = []
        
        if not hasattr(self, 'stage_h_pairs') or not self.stage_h_pairs:
            return
        
        for pair in self.stage_h_pairs:
            pair_frame = ctk.CTkFrame(self.stage_h_pairs_list_scroll)
            pair_frame.pack(fill="x", padx=5, pady=2)
            
            stage_j_name = os.path.basename(pair['stage_j_path']) if pair['stage_j_path'] else "None"
            stage_f_name = os.path.basename(pair['stage_f_path']) if pair['stage_f_path'] else "None"
            
            pair_text = f"{stage_j_name} ↔ {stage_f_name}"
            
            name_label = ctk.CTkLabel(
                pair_frame,
                text=pair_text,
                font=ctk.CTkFont(size=11),
                anchor="w"
            )
            name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
            
            status_label = ctk.CTkLabel(
                pair_frame,
                text=pair.get('status', 'pending').upper(),
                font=ctk.CTkFont(size=10),
                text_color="gray" if pair.get('status') == 'pending' else 
                          "green" if pair.get('status') == 'completed' else "red"
            )
            status_label.pack(side="right", padx=10, pady=5)
            
            self.stage_h_pairs_info_list.append({
                'pair': pair,
                'status_label': status_label,
                'frame': pair_frame
            })
    
    def validate_stage_h_files(self):
        """Validate Stage H input files (kept for backward compatibility)"""
        # Validation is now done per pair in batch mode
        pass
    
    def process_stage_h_batch(self):
        """Process multiple Stage J + Stage F pairs for Stage H"""
        def worker():
            try:
                if not hasattr(self, 'stage_h_pairs') or not self.stage_h_pairs:
                    self.root.after(0, lambda: messagebox.showwarning(
                        "Warning",
                        "Please add files and create pairs first. Click 'Auto-Pair' button."
                    ))
                    return
                
                # Filter pairs that have both Stage J and Stage F file
                valid_pairs = [p for p in self.stage_h_pairs if p['stage_j_path'] and p['stage_f_path']]
                
                if not valid_pairs:
                    self.root.after(0, lambda: messagebox.showwarning(
                        "Warning",
                        "No valid pairs found. Each pair must have both Stage J and Stage F file."
                    ))
                    return
                
                self.stage_h_process_btn.configure(
                    state="disabled",
                    text="Processing Batch..."
                )
                
                # Get prompt
                prompt = self.stage_h_prompt_text.get("1.0", tk.END).strip()
                if not prompt:
                    default_prompt = self.prompt_manager.get_prompt("Flashcard Prompt")
                    if default_prompt:
                        prompt = default_prompt
                        self.logger.info("Using default Flashcard prompt from prompts.json")
                    else:
                        self.root.after(0, lambda: messagebox.showerror("Error", "Please enter a prompt"))
                        return
                
                # Get model
                model_name = self.stage_h_model_var.get() if hasattr(self, 'stage_h_model_var') else "gemini-2.5-pro"
                
                # Get delay
                try:
                    delay_seconds = float(self.stage_h_delay_var.get() if hasattr(self, 'stage_h_delay_var') else "5")
                    if delay_seconds < 0:
                        delay_seconds = 0
                except:
                    delay_seconds = 5
                
                total_pairs = len(valid_pairs)
                completed = 0
                failed = 0
                
                # Reset progress bar
                self.root.after(0, lambda: self.stage_h_progress_bar.set(0))
                
                # Process each pair
                for idx, pair in enumerate(valid_pairs):
                    stage_j_path = pair['stage_j_path']
                    stage_f_path = pair['stage_f_path']
                    
                    stage_j_name = os.path.basename(stage_j_path)
                    stage_f_name = os.path.basename(stage_f_path)
                    
                    # Update status to processing
                    if hasattr(self, 'stage_h_pairs_info_list'):
                        for pair_info in self.stage_h_pairs_info_list:
                            if pair_info['pair'] == pair:
                                self.root.after(0, lambda sl=pair_info['status_label']:
                                               sl.configure(text="PROCESSING", text_color="blue"))
                                break
                    
                    # Update progress label
                    self.root.after(0, lambda idx=idx, total=total_pairs, sj=stage_j_name, sf=stage_f_name:
                                   self.stage_h_status_label.configure(
                                       text=f"Processing pair {idx+1}/{total}: {sj} ↔ {sf}"))
                    
                    # Progress bar
                    progress = idx / total_pairs
                    self.root.after(0, lambda p=progress: self.stage_h_progress_bar.set(p))
                    
                    try:
                        def progress_callback(msg: str):
                            self.root.after(0, lambda m=msg:
                                           self.stage_h_status_label.configure(text=m))
                        
                        # Process Stage H
                        output_path = self.stage_h_processor.process_stage_h(
                            stage_j_path=stage_j_path,
                            stage_f_path=stage_f_path,
                            prompt=prompt,
                            model_name=model_name,
                            output_dir=self.get_default_output_dir(stage_j_path),
                            progress_callback=progress_callback
                        )
                        
                        if output_path and os.path.exists(output_path):
                            completed += 1
                            pair['output_path'] = output_path
                            pair['status'] = 'completed'
                            
                            # Update status to completed
                            if hasattr(self, 'stage_h_pairs_info_list'):
                                for pair_info in self.stage_h_pairs_info_list:
                                    if pair_info['pair'] == pair:
                                        self.root.after(0, lambda sl=pair_info['status_label']:
                                                       sl.configure(text="COMPLETED", text_color="green"))
                                        break
                            
                            self.logger.info(
                                f"Successfully processed: {stage_j_name} ↔ {stage_f_name} -> {os.path.basename(output_path)}"
                            )
                            
                            # Update last_stage_h_path to the last completed output
                            self.last_stage_h_path = output_path
                        else:
                            failed += 1
                            pair['status'] = 'failed'
                            pair['error'] = "Processing returned no output"
                            
                            # Update status to failed
                            if hasattr(self, 'stage_h_pairs_info_list'):
                                for pair_info in self.stage_h_pairs_info_list:
                                    if pair_info['pair'] == pair:
                                        self.root.after(0, lambda sl=pair_info['status_label']:
                                                       sl.configure(text="FAILED", text_color="red"))
                                        break
                            
                            self.logger.error(f"Failed to process: {stage_j_name} ↔ {stage_f_name}")
                            
                    except Exception as e:
                        failed += 1
                        pair['status'] = 'failed'
                        pair['error'] = str(e)
                        self.logger.error(f"Error processing {stage_j_name} ↔ {stage_f_name}: {str(e)}", exc_info=True)
                        
                        # Update status to failed
                        if hasattr(self, 'stage_h_pairs_info_list'):
                            for pair_info in self.stage_h_pairs_info_list:
                                if pair_info['pair'] == pair:
                                    self.root.after(0, lambda sl=pair_info['status_label']:
                                                   sl.configure(text="FAILED", text_color="red"))
                                    break
                    
                    # Delay before next batch (except for the last one)
                    if idx < total_pairs - 1 and delay_seconds > 0:
                        self.root.after(0, lambda: self.stage_h_status_label.configure(
                            text=f"Waiting {delay_seconds} seconds before next batch..."))
                        import time
                        time.sleep(delay_seconds)
                
                # Final progress update
                self.root.after(0, lambda: self.stage_h_progress_bar.set(1.0))
                
                self.root.after(0, lambda: self.stage_h_status_label.configure(
                    text=f"Batch completed: {completed} succeeded, {failed} failed",
                    text_color="green" if failed == 0 else "orange"
                ))
                
                # Show summary
                self.root.after(0, lambda: messagebox.showinfo(
                    "Batch Processing Complete",
                    f"Stage H batch completed!\n\n"
                    f"Total pairs: {total_pairs}\n"
                    f"Successful: {completed}\n"
                    f"Failed: {failed}"
                ))
                
                # Enable view CSV button if at least one succeeded
                if completed > 0:
                    self.root.after(0, lambda: self.stage_h_view_csv_btn.configure(state="normal"))
            
            except Exception as e:
                self.logger.error(f"Error in batch Stage H processing: {str(e)}", exc_info=True)
                self.root.after(0, lambda: messagebox.showerror(
                    "Error",
                    f"Batch processing error:\n{str(e)}"
                ))
            finally:
                self.root.after(0, lambda: self.stage_h_process_btn.configure(
                    state="normal",
                    text="Process All Pairs"
                ))
        
        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def process_stage_h(self):
        """Process Stage H (redirects to batch processing)"""
        # Redirect to batch processing
        self.process_stage_h_batch()
    
    def setup_stage_h_ui(self, parent):
        """Setup UI for Stage H: Flashcard Generation"""
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
        
        title = ctk.CTkLabel(main_frame, text="Flashcard Generation", 
                            font=ctk.CTkFont(size=24, weight="bold"))
        title.pack(pady=(0, 20))
        
        desc = ctk.CTkLabel(main_frame, text="Generate flashcards from Tagged data and Image File Catalog (Batch Processing).", 
                           font=ctk.CTkFont(size=12), text_color="gray")
        desc.pack(pady=(0, 20))
        
        # Stage J Files Selection - Batch Processing
        stage_j_frame = ctk.CTkFrame(main_frame)
        stage_j_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(
            stage_j_frame,
            text="Stage J JSON Files (Tagged Data) - Batch Processing",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=5)
        
        # File selection buttons for Stage J
        buttons_frame_j = ctk.CTkFrame(stage_j_frame)
        buttons_frame_j.pack(fill="x", padx=10, pady=5)
        
        def browse_multiple_stage_j_files():
            filenames = filedialog.askopenfilenames(
                title="Select Stage J JSON files",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            if filenames:
                if not hasattr(self, 'stage_h_selected_stage_j_files'):
                    self.stage_h_selected_stage_j_files = []
                for filename in filenames:
                    if filename not in self.stage_h_selected_stage_j_files:
                        self.stage_h_selected_stage_j_files.append(filename)
                        self._add_stage_h_stage_j_file_to_ui(filename)
                self._update_stage_h_pairs()
        
        def select_folder_stage_j_files():
            folder_path = filedialog.askdirectory(
                title="Select folder containing Stage J JSON files"
            )
            if not folder_path:
                return
            
            import glob
            json_files = glob.glob(os.path.join(folder_path, "*.json"))
            json_files = [f for f in json_files if os.path.isfile(f)]
            
            if not json_files:
                messagebox.showinfo("Info", "No JSON files found in selected folder")
                return
            
            if not hasattr(self, 'stage_h_selected_stage_j_files'):
                self.stage_h_selected_stage_j_files = []
            
            added_count = 0
            for json_file in json_files:
                if json_file not in self.stage_h_selected_stage_j_files:
                    self.stage_h_selected_stage_j_files.append(json_file)
                    self._add_stage_h_stage_j_file_to_ui(json_file)
                    added_count += 1
            
            if added_count > 0:
                self._update_stage_h_pairs()
            messagebox.showinfo("Success", f"Added {added_count} JSON file(s) from folder")
        
        ctk.CTkButton(
            buttons_frame_j,
            text="Browse Multiple Stage J Files",
            command=browse_multiple_stage_j_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        ctk.CTkButton(
            buttons_frame_j,
            text="Select Folder (Stage J)",
            command=select_folder_stage_j_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        # Stage J files list (scrollable)
        list_frame_j = ctk.CTkFrame(stage_j_frame)
        list_frame_j.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            list_frame_j,
            text="Selected Stage J Files:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_h_stage_j_files_list_scroll'):
            self.stage_h_stage_j_files_list_scroll = ctk.CTkScrollableFrame(list_frame_j, height=100)
        self.stage_h_stage_j_files_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize selected files list
        if not hasattr(self, 'stage_h_selected_stage_j_files'):
            self.stage_h_selected_stage_j_files = []
        
        # Stage F Files Selection - Batch Processing
        stage_f_frame = ctk.CTkFrame(main_frame)
        stage_f_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(
            stage_f_frame,
            text="Stage F JSON Files (Image File Catalog) - Batch Processing",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=5)
        
        # File selection buttons for Stage F
        buttons_frame_f = ctk.CTkFrame(stage_f_frame)
        buttons_frame_f.pack(fill="x", padx=10, pady=5)
        
        def browse_multiple_stage_f_files():
            filenames = filedialog.askopenfilenames(
                title="Select Stage F JSON files",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            if filenames:
                if not hasattr(self, 'stage_h_selected_stage_f_files'):
                    self.stage_h_selected_stage_f_files = []
                for filename in filenames:
                    if filename not in self.stage_h_selected_stage_f_files:
                        self.stage_h_selected_stage_f_files.append(filename)
                        self._add_stage_h_stage_f_file_to_ui(filename)
                self._update_stage_h_pairs()
        
        def select_folder_stage_f_files():
            folder_path = filedialog.askdirectory(
                title="Select folder containing Stage F JSON files"
            )
            if not folder_path:
                return
            
            import glob
            json_files = glob.glob(os.path.join(folder_path, "*.json"))
            json_files = [f for f in json_files if os.path.isfile(f)]
            
            if not json_files:
                messagebox.showinfo("Info", "No JSON files found in selected folder")
                return
            
            if not hasattr(self, 'stage_h_selected_stage_f_files'):
                self.stage_h_selected_stage_f_files = []
            
            added_count = 0
            for json_file in json_files:
                if json_file not in self.stage_h_selected_stage_f_files:
                    self.stage_h_selected_stage_f_files.append(json_file)
                    self._add_stage_h_stage_f_file_to_ui(json_file)
                    added_count += 1
            
            if added_count > 0:
                self._update_stage_h_pairs()
            messagebox.showinfo("Success", f"Added {added_count} JSON file(s) from folder")
        
        ctk.CTkButton(
            buttons_frame_f,
            text="Browse Multiple Stage F Files",
            command=browse_multiple_stage_f_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        ctk.CTkButton(
            buttons_frame_f,
            text="Select Folder (Stage F)",
            command=select_folder_stage_f_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        # Stage F files list (scrollable)
        list_frame_f = ctk.CTkFrame(stage_f_frame)
        list_frame_f.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            list_frame_f,
            text="Selected Stage F Files:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_h_stage_f_files_list_scroll'):
            self.stage_h_stage_f_files_list_scroll = ctk.CTkScrollableFrame(list_frame_f, height=100)
        self.stage_h_stage_f_files_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize selected files list
        if not hasattr(self, 'stage_h_selected_stage_f_files'):
            self.stage_h_selected_stage_f_files = []
        
        # Pairs Section
        pairs_frame = ctk.CTkFrame(main_frame)
        pairs_frame.pack(fill="x", pady=10)
        
        pairs_header_frame = ctk.CTkFrame(pairs_frame)
        pairs_header_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(
            pairs_header_frame,
            text="Pairs (Stage J ↔ Stage F File):",
            font=ctk.CTkFont(size=14, weight="bold"),
        ).pack(side="left", padx=10, pady=5)
        
        ctk.CTkButton(
            pairs_header_frame,
            text="Auto-Pair",
            command=self._auto_pair_stage_h_files,
            width=120,
            height=30,
            fg_color="green",
            hover_color="darkgreen"
        ).pack(side="right", padx=10, pady=5)
        
        # Pairs list (scrollable)
        pairs_list_frame = ctk.CTkFrame(pairs_frame)
        pairs_list_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            pairs_list_frame,
            text="File Pairs:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_h_pairs_list_scroll'):
            self.stage_h_pairs_list_scroll = ctk.CTkScrollableFrame(pairs_list_frame, height=150)
        self.stage_h_pairs_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize pairs
        if not hasattr(self, 'stage_h_pairs'):
            self.stage_h_pairs = []
        if not hasattr(self, 'stage_h_pairs_info_list'):
            self.stage_h_pairs_info_list = []
        
        # Prompt Section
        prompt_frame = ctk.CTkFrame(main_frame)
        prompt_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(
            prompt_frame,
            text="Prompt for Flashcard Generation:",
            font=ctk.CTkFont(size=14, weight="bold"),
        ).pack(anchor="w", padx=10, pady=5)

        # Stage H prompt mode (default vs custom)
        stage_h_mode_frame = ctk.CTkFrame(prompt_frame)
        stage_h_mode_frame.pack(fill="x", padx=10, pady=(0, 5))
        if not hasattr(self, 'stage_h_prompt_type_var'):
            self.stage_h_prompt_type_var = ctk.StringVar(value="default")
        ctk.CTkRadioButton(
            stage_h_mode_frame,
            text="Use Default Prompt",
            variable=self.stage_h_prompt_type_var,
            value="default",
            command=self.on_stage_h_prompt_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)
        ctk.CTkRadioButton(
            stage_h_mode_frame,
            text="Use Custom Prompt",
            variable=self.stage_h_prompt_type_var,
            value="custom",
            command=self.on_stage_h_prompt_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)

        # Default Stage H prompt combobox (predefined prompts)
        stage_h_default_frame = ctk.CTkFrame(prompt_frame)
        stage_h_default_frame.pack(fill="x", padx=10, pady=(0, 10))

        ctk.CTkLabel(
            stage_h_default_frame,
            text="Default Stage H Prompt:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", pady=(0, 5))

        stage_h_prompt_names = self.prompt_manager.get_prompt_names()
        preferred_stage_h_name = "Flashcard Prompt"
        if preferred_stage_h_name in stage_h_prompt_names:
            stage_h_default_value = preferred_stage_h_name
        else:
            stage_h_default_value = stage_h_prompt_names[0] if stage_h_prompt_names else ""
        self.stage_h_default_prompt_var = ctk.StringVar(value=stage_h_default_value)
        self.stage_h_default_prompt_combo = ctk.CTkComboBox(
            stage_h_default_frame,
            values=stage_h_prompt_names,
            variable=self.stage_h_default_prompt_var,
            width=400,
            command=self.on_stage_h_default_prompt_selected,
        )
        self.stage_h_default_prompt_combo.pack(anchor="w", pady=(0, 5))

        # Textbox for Stage H prompt (default-filled or custom)
        if not hasattr(self, 'stage_h_prompt_text'):
            self.stage_h_prompt_text = ctk.CTkTextbox(prompt_frame, height=150, font=self.farsi_text_font)
        else:
            try:
                self.stage_h_prompt_text.pack_forget()
            except Exception:
                pass
        self.stage_h_prompt_text.pack(fill="x", padx=10, pady=5)

        # Apply initial default/custom state and fill textbox
        self.on_stage_h_prompt_type_change()
        
        # Model Selection
        model_frame = ctk.CTkFrame(main_frame)
        model_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(model_frame, text="Model:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        # Get default model from main model selection if available
        default_model = "gemini-2.5-pro"
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
        
        # Delay Setting
        delay_frame = ctk.CTkFrame(main_frame)
        delay_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(delay_frame, text="Delay Between Batches (seconds):", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_h_delay_var'):
            self.stage_h_delay_var = ctk.StringVar(value="5")
        
        delay_entry_frame = ctk.CTkFrame(delay_frame)
        delay_entry_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(delay_entry_frame, text="Delay:").pack(side="left", padx=5)
        delay_entry = ctk.CTkEntry(delay_entry_frame, textvariable=self.stage_h_delay_var, width=100)
        delay_entry.pack(side="left", padx=5)
        ctk.CTkLabel(delay_entry_frame, text="seconds", text_color="gray").pack(side="left", padx=5)
        
        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)
        
        if not hasattr(self, 'stage_h_process_btn'):
            self.stage_h_process_btn = ctk.CTkButton(
                process_btn_frame,
                text="Process All Pairs",
                command=self.process_stage_h_batch,
                font=ctk.CTkFont(size=16, weight="bold"),
                height=40
            )
        self.stage_h_process_btn.pack(pady=10)
        
        # Progress bar for batch processing
        if not hasattr(self, 'stage_h_progress_bar'):
            self.stage_h_progress_bar = ctk.CTkProgressBar(process_btn_frame, width=400)
        self.stage_h_progress_bar.pack(pady=10)
        self.stage_h_progress_bar.set(0)
        
        # View CSV button
        if not hasattr(self, 'stage_h_view_csv_btn'):
            self.stage_h_view_csv_btn = ctk.CTkButton(
                process_btn_frame,
                text="View CSV",
                command=self.view_csv_stage_h,
                width=150,
                height=40,
                font=ctk.CTkFont(size=14),
                fg_color="green",
                hover_color="darkgreen",
                state="disabled"
            )
        self.stage_h_view_csv_btn.pack(pady=10)
        
        # Status Label
        if not hasattr(self, 'stage_h_status_label'):
            self.stage_h_status_label = ctk.CTkLabel(
                process_btn_frame,
                text="Ready",
                font=ctk.CTkFont(size=12),
                text_color="gray"
            )
        self.stage_h_status_label.pack(pady=5)
    
    def setup_stage_v_ui(self, parent):
        """Setup UI for Stage V: Test File Generation"""
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
        
        title = ctk.CTkLabel(main_frame, text="Test Bank Generation", 
                            font=ctk.CTkFont(size=24, weight="bold"))
        title.pack(pady=(0, 20))
        
        desc = ctk.CTkLabel(main_frame, text="Generate test files from Tagged data and Word document (Batch Processing).", 
                           font=ctk.CTkFont(size=12), text_color="gray")
        desc.pack(pady=(0, 20))
        
        # Stage J Files Selection - Batch Processing
        stage_j_frame = ctk.CTkFrame(main_frame)
        stage_j_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(
            stage_j_frame,
            text="Stage J JSON Files (Tagged Data) - Batch Processing",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=5)
        
        # File selection buttons for Stage J
        buttons_frame_j = ctk.CTkFrame(stage_j_frame)
        buttons_frame_j.pack(fill="x", padx=10, pady=5)
        
        def browse_multiple_stage_j_files():
            filenames = filedialog.askopenfilenames(
                title="Select Stage J JSON files",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            if filenames:
                if not hasattr(self, 'stage_v_selected_stage_j_files'):
                    self.stage_v_selected_stage_j_files = []
                for filename in filenames:
                    if filename not in self.stage_v_selected_stage_j_files:
                        self.stage_v_selected_stage_j_files.append(filename)
                        self._add_stage_v_stage_j_file_to_ui(filename)
                self._update_stage_v_pairs()
        
        def select_folder_stage_j_files():
            folder_path = filedialog.askdirectory(
                title="Select folder containing Stage J JSON files"
            )
            if not folder_path:
                return
            
            import glob
            json_files = glob.glob(os.path.join(folder_path, "*.json"))
            json_files = [f for f in json_files if os.path.isfile(f)]
            
            if not json_files:
                messagebox.showinfo("Info", "No JSON files found in selected folder")
                return
            
            if not hasattr(self, 'stage_v_selected_stage_j_files'):
                self.stage_v_selected_stage_j_files = []
            
            added_count = 0
            for json_file in json_files:
                if json_file not in self.stage_v_selected_stage_j_files:
                    self.stage_v_selected_stage_j_files.append(json_file)
                    self._add_stage_v_stage_j_file_to_ui(json_file)
                    added_count += 1
            
            if added_count > 0:
                self._update_stage_v_pairs()
            messagebox.showinfo("Success", f"Added {added_count} JSON file(s) from folder")
        
        ctk.CTkButton(
            buttons_frame_j,
            text="Browse Multiple Stage J Files",
            command=browse_multiple_stage_j_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        ctk.CTkButton(
            buttons_frame_j,
            text="Select Folder (Stage J)",
            command=select_folder_stage_j_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        # Stage J files list (scrollable)
        list_frame_j = ctk.CTkFrame(stage_j_frame)
        list_frame_j.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            list_frame_j,
            text="Selected Stage J Files:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_v_stage_j_files_list_scroll'):
            self.stage_v_stage_j_files_list_scroll = ctk.CTkScrollableFrame(list_frame_j, height=100)
        self.stage_v_stage_j_files_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize selected files list
        if not hasattr(self, 'stage_v_selected_stage_j_files'):
            self.stage_v_selected_stage_j_files = []
        
        # Word Files Selection - Batch Processing
        word_frame = ctk.CTkFrame(main_frame)
        word_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(
            word_frame,
            text="Word Files (Test Questions) - Batch Processing",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=5)
        
        # File selection buttons for Word files
        buttons_frame_word = ctk.CTkFrame(word_frame)
        buttons_frame_word.pack(fill="x", padx=10, pady=5)
        
        def browse_multiple_word_files():
            filenames = filedialog.askopenfilenames(
                title="Select Word files",
                filetypes=[("Word Documents", "*.docx *.doc"), ("All files", "*.*")]
            )
            if filenames:
                if not hasattr(self, 'stage_v_selected_word_files'):
                    self.stage_v_selected_word_files = []
                for filename in filenames:
                    if filename not in self.stage_v_selected_word_files:
                        self.stage_v_selected_word_files.append(filename)
                        self._add_stage_v_word_file_to_ui(filename)
                self._update_stage_v_pairs()
        
        def select_folder_word_files():
            folder_path = filedialog.askdirectory(
                title="Select folder containing Word files"
            )
            if not folder_path:
                return
            
            import glob
            word_files = glob.glob(os.path.join(folder_path, "*.docx")) + glob.glob(os.path.join(folder_path, "*.doc"))
            word_files = [f for f in word_files if os.path.isfile(f)]
            
            if not word_files:
                messagebox.showinfo("Info", "No Word files found in selected folder")
                return
            
            if not hasattr(self, 'stage_v_selected_word_files'):
                self.stage_v_selected_word_files = []
            
            added_count = 0
            for word_file in word_files:
                if word_file not in self.stage_v_selected_word_files:
                    self.stage_v_selected_word_files.append(word_file)
                    self._add_stage_v_word_file_to_ui(word_file)
                    added_count += 1
            
            if added_count > 0:
                self._update_stage_v_pairs()
            messagebox.showinfo("Success", f"Added {added_count} Word file(s) from folder")
        
        ctk.CTkButton(
            buttons_frame_word,
            text="Browse Multiple Word Files",
            command=browse_multiple_word_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        ctk.CTkButton(
            buttons_frame_word,
            text="Select Folder (Word)",
            command=select_folder_word_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        # Word files list (scrollable)
        list_frame_word = ctk.CTkFrame(word_frame)
        list_frame_word.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            list_frame_word,
            text="Selected Word Files:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_v_word_files_list_scroll'):
            self.stage_v_word_files_list_scroll = ctk.CTkScrollableFrame(list_frame_word, height=100)
        self.stage_v_word_files_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize selected files list
        if not hasattr(self, 'stage_v_selected_word_files'):
            self.stage_v_selected_word_files = []
        
        # OCR Extraction JSON File Selection (Optional - Single File or Auto-Detect)
        ocr_extraction_frame = ctk.CTkFrame(main_frame)
        ocr_extraction_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(ocr_extraction_frame, text="OCR Extraction JSON File (Optional - Auto-detected per pair if not specified):", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_v_ocr_extraction_var'):
            self.stage_v_ocr_extraction_var = ctk.StringVar()
            # Auto-fill if available
            if hasattr(self, 'last_ocr_extraction_path') and self.last_ocr_extraction_path and os.path.exists(self.last_ocr_extraction_path):
                self.stage_v_ocr_extraction_var.set(self.last_ocr_extraction_path)
        
        entry_frame_ocr = ctk.CTkFrame(ocr_extraction_frame)
        entry_frame_ocr.pack(fill="x", padx=10, pady=5)
        
        ocr_extraction_entry = ctk.CTkEntry(entry_frame_ocr, textvariable=self.stage_v_ocr_extraction_var, width=400)
        ocr_extraction_entry.pack(side="left", fill="x", expand=True, padx=5)
        
        ctk.CTkButton(entry_frame_ocr, text="Browse", 
                     command=lambda: self.browse_file_for_stage(self.stage_v_ocr_extraction_var, 
                                                                 filetypes=[("JSON files", "*.json"), ("All files", "*.*")])).pack(side="right")
        
        ctk.CTkLabel(ocr_extraction_frame, text="Note: If not specified, OCR Extraction JSON will be auto-detected from Stage J directory for each pair.", 
                    font=ctk.CTkFont(size=11), text_color="gray").pack(anchor="w", padx=10, pady=(0, 5))
        
        # Pairs Section
        pairs_frame = ctk.CTkFrame(main_frame)
        pairs_frame.pack(fill="x", pady=10)
        
        pairs_header_frame = ctk.CTkFrame(pairs_frame)
        pairs_header_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(
            pairs_header_frame,
            text="Pairs (Stage J ↔ Word File):",
            font=ctk.CTkFont(size=14, weight="bold"),
        ).pack(side="left", padx=10, pady=5)
        
        ctk.CTkButton(
            pairs_header_frame,
            text="Auto-Pair",
            command=self._auto_pair_stage_v_files,
            width=120,
            height=30,
            fg_color="green",
            hover_color="darkgreen"
        ).pack(side="right", padx=10, pady=5)
        
        # Pairs list (scrollable)
        pairs_list_frame = ctk.CTkFrame(pairs_frame)
        pairs_list_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            pairs_list_frame,
            text="File Pairs:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_v_pairs_list_scroll'):
            self.stage_v_pairs_list_scroll = ctk.CTkScrollableFrame(pairs_list_frame, height=150)
        self.stage_v_pairs_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize pairs
        if not hasattr(self, 'stage_v_pairs'):
            self.stage_v_pairs = []
        if not hasattr(self, 'stage_v_pairs_info_list'):
            self.stage_v_pairs_info_list = []
        
        # Step 1 Section
        step1_frame = ctk.CTkFrame(main_frame)
        step1_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(step1_frame, text="Step 1: Initial Question Generation", 
                    font=ctk.CTkFont(size=16, weight="bold")).pack(anchor="w", padx=10, pady=10)
        
        # Step 1 Prompt
        ctk.CTkLabel(step1_frame, text="Prompt for Step 1:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)

        # Step 1 prompt mode (default vs custom)
        step1_mode_frame = ctk.CTkFrame(step1_frame)
        step1_mode_frame.pack(fill="x", padx=10, pady=(0, 5))
        if not hasattr(self, 'stage_v_prompt1_type_var'):
            self.stage_v_prompt1_type_var = ctk.StringVar(value="default")
        ctk.CTkRadioButton(
            step1_mode_frame,
            text="Use Default Prompt",
            variable=self.stage_v_prompt1_type_var,
            value="default",
            command=self.on_stage_v_prompt1_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)
        ctk.CTkRadioButton(
            step1_mode_frame,
            text="Use Custom Prompt",
            variable=self.stage_v_prompt1_type_var,
            value="custom",
            command=self.on_stage_v_prompt1_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)

        # Default prompt combobox for Step 1
        step1_default_frame = ctk.CTkFrame(step1_frame)
        step1_default_frame.pack(fill="x", padx=10, pady=(0, 10))

        ctk.CTkLabel(
            step1_default_frame,
            text="Default Prompt for Step 1:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", pady=(0, 5))

        step_prompt_names = self.prompt_manager.get_prompt_names()
        preferred_step1_name = "Test Bank Generation - Step 1 Prompt"
        if preferred_step1_name in step_prompt_names:
            step1_default_value = preferred_step1_name
        else:
            step1_default_value = step_prompt_names[0] if step_prompt_names else ""
        self.stage_v_prompt1_default_var = ctk.StringVar(value=step1_default_value)
        self.stage_v_prompt1_default_combo = ctk.CTkComboBox(
            step1_default_frame,
            values=step_prompt_names,
            variable=self.stage_v_prompt1_default_var,
            width=400,
            command=self.on_stage_v_prompt1_default_selected,
        )
        self.stage_v_prompt1_default_combo.pack(anchor="w", pady=(0, 5))

        # Textbox for Step 1 prompt
        if not hasattr(self, 'stage_v_prompt1_text'):
            self.stage_v_prompt1_text = ctk.CTkTextbox(step1_frame, height=150, font=self.farsi_text_font)
        else:
            try:
                self.stage_v_prompt1_text.pack_forget()
            except Exception:
                pass
        self.stage_v_prompt1_text.pack(fill="x", padx=10, pady=5)

        # Apply initial default/custom state and fill textbox
        self.on_stage_v_prompt1_type_change()
        
        # Step 1 Model Selection
        model1_frame = ctk.CTkFrame(step1_frame)
        model1_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(model1_frame, text="Model for Step 1:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(side="left", padx=5)
        
        default_model = "gemini-2.5-pro"
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

        # Step 2 prompt mode (default vs custom)
        step2_mode_frame = ctk.CTkFrame(step2_frame)
        step2_mode_frame.pack(fill="x", padx=10, pady=(0, 5))
        if not hasattr(self, 'stage_v_prompt2_type_var'):
            self.stage_v_prompt2_type_var = ctk.StringVar(value="default")
        ctk.CTkRadioButton(
            step2_mode_frame,
            text="Use Default Prompt",
            variable=self.stage_v_prompt2_type_var,
            value="default",
            command=self.on_stage_v_prompt2_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)
        ctk.CTkRadioButton(
            step2_mode_frame,
            text="Use Custom Prompt",
            variable=self.stage_v_prompt2_type_var,
            value="custom",
            command=self.on_stage_v_prompt2_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)

        # Default prompt combobox for Step 2
        step2_default_frame = ctk.CTkFrame(step2_frame)
        step2_default_frame.pack(fill="x", padx=10, pady=(0, 10))

        ctk.CTkLabel(
            step2_default_frame,
            text="Default Prompt for Step 2:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", pady=(0, 5))

        preferred_step2_name = "Test Bank Generation - Step 2 Prompt"
        if preferred_step2_name in step_prompt_names:
            step2_default_value = preferred_step2_name
        else:
            step2_default_value = step_prompt_names[0] if step_prompt_names else ""
        self.stage_v_prompt2_default_var = ctk.StringVar(value=step2_default_value)
        self.stage_v_prompt2_default_combo = ctk.CTkComboBox(
            step2_default_frame,
            values=step_prompt_names,
            variable=self.stage_v_prompt2_default_var,
            width=400,
            command=self.on_stage_v_prompt2_default_selected,
        )
        self.stage_v_prompt2_default_combo.pack(anchor="w", pady=(0, 5))

        # Textbox for Step 2 prompt
        if not hasattr(self, 'stage_v_prompt2_text'):
            self.stage_v_prompt2_text = ctk.CTkTextbox(step2_frame, height=150, font=self.farsi_text_font)
        else:
            try:
                self.stage_v_prompt2_text.pack_forget()
            except Exception:
                pass
        self.stage_v_prompt2_text.pack(fill="x", padx=10, pady=5)

        # Apply initial default/custom state and fill textbox
        self.on_stage_v_prompt2_type_change()
        
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
        
        # Delay Setting
        delay_frame = ctk.CTkFrame(main_frame)
        delay_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(delay_frame, text="Delay Between Batches (seconds):", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_v_delay_var'):
            self.stage_v_delay_var = ctk.StringVar(value="5")
        
        delay_entry_frame = ctk.CTkFrame(delay_frame)
        delay_entry_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(delay_entry_frame, text="Delay:").pack(side="left", padx=5)
        delay_entry = ctk.CTkEntry(delay_entry_frame, textvariable=self.stage_v_delay_var, width=100)
        delay_entry.pack(side="left", padx=5)
        ctk.CTkLabel(delay_entry_frame, text="seconds", text_color="gray").pack(side="left", padx=5)
        
        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)
        
        if not hasattr(self, 'stage_v_process_btn'):
            self.stage_v_process_btn = ctk.CTkButton(
                process_btn_frame,
                text="Process All Pairs",
                command=self.process_stage_v_batch,
                font=ctk.CTkFont(size=16, weight="bold"),
                height=40
            )
        self.stage_v_process_btn.pack(pady=10)
        
        # Progress bar for batch processing
        if not hasattr(self, 'stage_v_progress_bar'):
            self.stage_v_progress_bar = ctk.CTkProgressBar(process_btn_frame, width=400)
        self.stage_v_progress_bar.pack(pady=10)
        self.stage_v_progress_bar.set(0)
        
        # View CSV button
        if not hasattr(self, 'stage_v_view_csv_btn'):
            self.stage_v_view_csv_btn = ctk.CTkButton(
                process_btn_frame,
                text="View CSV",
                command=self.view_csv_stage_v,
                width=150,
                height=40,
                font=ctk.CTkFont(size=14),
                fg_color="green",
                hover_color="darkgreen",
                state="disabled"
            )
        self.stage_v_view_csv_btn.pack(pady=10)
        
        # Status Label
        if not hasattr(self, 'stage_v_status_label'):
            self.stage_v_status_label = ctk.CTkLabel(
                process_btn_frame,
                text="Ready",
                font=ctk.CTkFont(size=12),
                text_color="gray"
            )
        self.stage_v_status_label.pack(pady=5)
    
    def _add_stage_v_stage_j_file_to_ui(self, file_path: str):
        """Add a Stage J file to the UI list"""
        file_frame = ctk.CTkFrame(self.stage_v_stage_j_files_list_scroll)
        file_frame.pack(fill="x", padx=5, pady=2)
        
        file_name = os.path.basename(file_path)
        
        name_label = ctk.CTkLabel(
            file_frame,
            text=file_name,
            font=ctk.CTkFont(size=11),
            anchor="w"
        )
        name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
        
        def remove_file():
            if file_path in self.stage_v_selected_stage_j_files:
                self.stage_v_selected_stage_j_files.remove(file_path)
            file_frame.destroy()
            self._update_stage_v_pairs()
        
        remove_btn = ctk.CTkButton(
            file_frame,
            text="✕",
            command=remove_file,
            width=30,
            height=25,
            fg_color="red",
            hover_color="darkred"
        )
        remove_btn.pack(side="right", padx=5, pady=5)
    
    def _add_stage_v_word_file_to_ui(self, file_path: str):
        """Add a Word file to the UI list"""
        file_frame = ctk.CTkFrame(self.stage_v_word_files_list_scroll)
        file_frame.pack(fill="x", padx=5, pady=2)
        
        file_name = os.path.basename(file_path)
        
        name_label = ctk.CTkLabel(
            file_frame,
            text=file_name,
            font=ctk.CTkFont(size=11),
            anchor="w"
        )
        name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
        
        def remove_file():
            if file_path in self.stage_v_selected_word_files:
                self.stage_v_selected_word_files.remove(file_path)
            file_frame.destroy()
            self._update_stage_v_pairs()
        
        remove_btn = ctk.CTkButton(
            file_frame,
            text="✕",
            command=remove_file,
            width=30,
            height=25,
            fg_color="red",
            hover_color="darkred"
        )
        remove_btn.pack(side="right", padx=5, pady=5)
    
    def _extract_book_chapter_from_stage_j_for_v(self, stage_j_path: str):
        """Extract book and chapter from Stage J file (from PointId or filename)"""
        try:
            # Try to load Stage J and extract from PointId
            data = json.load(open(stage_j_path, 'r', encoding='utf-8'))
            records = data.get("data") or data.get("rows", [])
            if records and records[0].get("PointId"):
                point_id = records[0].get("PointId")
                if isinstance(point_id, str) and len(point_id) >= 6:
                    book_id = int(point_id[0:3])
                    chapter_id = int(point_id[3:6])
                    return book_id, chapter_id
        except:
            pass
        
        # Fallback: try to extract from filename (a{book}{chapter}.json)
        try:
            basename = os.path.basename(stage_j_path)
            name_without_ext = os.path.splitext(basename)[0]
            if name_without_ext.startswith('a') and len(name_without_ext) >= 7:
                book_chapter = name_without_ext[1:]
                book_id = int(book_chapter[0:3])
                chapter_id = int(book_chapter[3:6])
                return book_id, chapter_id
        except:
            pass
        
        return None, None
    
    def _extract_book_chapter_from_word_filename_for_v(self, word_path: str):
        """Extract book and chapter from Word filename (various patterns)"""
        import re
        basename = os.path.basename(word_path)
        name_without_ext = os.path.splitext(basename)[0]
        
        # Try various patterns
        patterns = [
            r'ch(\d{3})_(\d{3})',  # ch105_003.docx
            r'chapter_(\d{3})_(\d{3})',  # chapter_105_003.docx
            r'(\d{3})_(\d{3})',  # 105_003.docx
            r'book(\d{3})_chapter(\d{3})',  # book105_chapter003.docx
            r'e(\d{3})(\d{3})',  # e105003.docx
        ]
        
        for pattern in patterns:
            match = re.search(pattern, name_without_ext, re.IGNORECASE)
            if match:
                book_id = int(match.group(1))
                chapter_id = int(match.group(2))
                return book_id, chapter_id
        
        return None, None
    
    def _auto_detect_ocr_extraction_for_pair(self, stage_j_path: str, common_ocr_path: Optional[str] = None):
        """Auto-detect OCR Extraction JSON file for a Stage J file"""
        # If common OCR path is provided, use it
        if common_ocr_path and os.path.exists(common_ocr_path):
            return common_ocr_path
        
        # Try to find OCR Extraction JSON in same directory as Stage J
        stage_j_dir = os.path.dirname(stage_j_path)
        stage_j_basename = os.path.basename(stage_j_path)
        stage_j_name_without_ext = os.path.splitext(stage_j_basename)[0]
        
        # Try various patterns
        possible_names = [
            "OCR Extraction.json",
            f"OCR Extraction_{stage_j_name_without_ext}.json",
            "OCR Extraction_*.json",  # Will use glob
        ]
        
        # Extract book/chapter from Stage J filename
        book_id, chapter_id = self._extract_book_chapter_from_stage_j_for_v(stage_j_path)
        if book_id and chapter_id:
            possible_names.extend([
                f"OCR Extraction_{book_id:03d}{chapter_id:03d}.json",
                f"OCR_Extraction_{book_id:03d}{chapter_id:03d}.json",
            ])
        
        # Try to find matching file
        import glob
        for pattern in possible_names:
            if '*' in pattern:
                # Use glob
                matches = glob.glob(os.path.join(stage_j_dir, pattern))
                if matches:
                    return matches[0]
            else:
                # Direct file
                file_path = os.path.join(stage_j_dir, pattern)
                if os.path.exists(file_path):
                    return file_path
        
        return None
    
    def _auto_pair_stage_v_files(self):
        """Auto-pair Stage J files with Word files based on Book/Chapter"""
        if not hasattr(self, 'stage_v_selected_stage_j_files') or not self.stage_v_selected_stage_j_files:
            messagebox.showwarning("Warning", "Please add at least one Stage J file")
            return
        
        if not hasattr(self, 'stage_v_selected_word_files') or not self.stage_v_selected_word_files:
            messagebox.showwarning("Warning", "Please add at least one Word file")
            return
        
        # Get common OCR Extraction file if specified
        common_ocr_path = None
        if hasattr(self, 'stage_v_ocr_extraction_var') and self.stage_v_ocr_extraction_var.get():
            ocr_path = self.stage_v_ocr_extraction_var.get().strip()
            if ocr_path and os.path.exists(ocr_path):
                common_ocr_path = ocr_path
        
        pairs = []
        paired_word_files = set()
        
        for stage_j_path in self.stage_v_selected_stage_j_files:
            book_id, chapter_id = self._extract_book_chapter_from_stage_j_for_v(stage_j_path)
            
            if book_id is None or chapter_id is None:
                # Can't extract book/chapter, skip
                continue
            
            # Find matching Word file
            matched_word = None
            for word_path in self.stage_v_selected_word_files:
                if word_path in paired_word_files:
                    continue
                
                word_book, word_chapter = self._extract_book_chapter_from_word_filename_for_v(word_path)
                
                if word_book == book_id and word_chapter == chapter_id:
                    matched_word = word_path
                    paired_word_files.add(word_path)
                    break
            
            # Auto-detect OCR Extraction JSON
            ocr_extraction_path = self._auto_detect_ocr_extraction_for_pair(stage_j_path, common_ocr_path)
            
            pair = {
                'stage_j_path': stage_j_path,
                'word_path': matched_word,
                'ocr_extraction_path': ocr_extraction_path,
                'status': 'pending',
                'output_path': None,
                'error': None
            }
            pairs.append(pair)
        
        self.stage_v_pairs = pairs
        self._update_stage_v_pairs_ui()
        
        paired_count = sum(1 for p in pairs if p['word_path'] is not None)
        unpaired_count = len(pairs) - paired_count
        ocr_detected_count = sum(1 for p in pairs if p['ocr_extraction_path'] is not None)
        
        messagebox.showinfo(
            "Auto-Pairing Complete",
            f"Paired: {paired_count}\nUnpaired: {unpaired_count}\nOCR Auto-detected: {ocr_detected_count}"
        )
    
    def _update_stage_v_pairs(self):
        """Update pairs when files are added/removed"""
        # Clear existing pairs UI
        for widget in self.stage_v_pairs_list_scroll.winfo_children():
            widget.destroy()
        
        self.stage_v_pairs_info_list = []
        
        # Re-pair if we have pairs
        if hasattr(self, 'stage_v_pairs') and self.stage_v_pairs:
            self._update_stage_v_pairs_ui()
    
    def _update_stage_v_pairs_ui(self):
        """Update the pairs UI display"""
        # Clear existing pairs UI
        for widget in self.stage_v_pairs_list_scroll.winfo_children():
            widget.destroy()
        
        self.stage_v_pairs_info_list = []
        
        if not hasattr(self, 'stage_v_pairs') or not self.stage_v_pairs:
            return
        
        for pair in self.stage_v_pairs:
            pair_frame = ctk.CTkFrame(self.stage_v_pairs_list_scroll)
            pair_frame.pack(fill="x", padx=5, pady=2)
            
            stage_j_name = os.path.basename(pair['stage_j_path']) if pair['stage_j_path'] else "None"
            word_name = os.path.basename(pair['word_path']) if pair['word_path'] else "None"
            ocr_name = os.path.basename(pair['ocr_extraction_path']) if pair['ocr_extraction_path'] else "Auto-detect"
            
            pair_text = f"{stage_j_name} ↔ {word_name}"
            ocr_text = f"OCR: {ocr_name}"
            
            # Main pair info
            info_frame = ctk.CTkFrame(pair_frame)
            info_frame.pack(fill="x", padx=5, pady=2)
            
            name_label = ctk.CTkLabel(
                info_frame,
                text=pair_text,
                font=ctk.CTkFont(size=11),
                anchor="w"
            )
            name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
            
            status_label = ctk.CTkLabel(
                info_frame,
                text=pair.get('status', 'pending').upper(),
                font=ctk.CTkFont(size=10),
                text_color="gray" if pair.get('status') == 'pending' else 
                          "green" if pair.get('status') == 'completed' else "red"
            )
            status_label.pack(side="right", padx=10, pady=5)
            
            # OCR info
            ocr_label = ctk.CTkLabel(
                pair_frame,
                text=ocr_text,
                font=ctk.CTkFont(size=9),
                text_color="gray",
                anchor="w"
            )
            ocr_label.pack(fill="x", padx=15, pady=(0, 5))
            
            self.stage_v_pairs_info_list.append({
                'pair': pair,
                'status_label': status_label,
                'frame': pair_frame
            })
    
    def validate_stage_v_files(self):
        """Validate Stage V input files (kept for backward compatibility)"""
        # Validation is now done per pair in batch mode
        pass
    
    def process_stage_v_batch(self):
        """Process multiple Stage J + Word file pairs for Stage V"""
        def worker():
            try:
                if not hasattr(self, 'stage_v_pairs') or not self.stage_v_pairs:
                    self.root.after(0, lambda: messagebox.showwarning(
                        "Warning",
                        "Please add files and create pairs first. Click 'Auto-Pair' button."
                    ))
                    return
                
                # Filter pairs that have both Stage J and Word file
                valid_pairs = [p for p in self.stage_v_pairs if p['stage_j_path'] and p['word_path']]
                
                if not valid_pairs:
                    self.root.after(0, lambda: messagebox.showwarning(
                        "Warning",
                        "No valid pairs found. Each pair must have both Stage J and Word file."
                    ))
                    return
                
                # Check OCR Extraction for all pairs
                pairs_without_ocr = [p for p in valid_pairs if not p.get('ocr_extraction_path')]
                if pairs_without_ocr:
                    self.root.after(0, lambda: messagebox.showwarning(
                        "Warning",
                        f"{len(pairs_without_ocr)} pair(s) do not have OCR Extraction JSON.\n"
                        "Please specify a common OCR Extraction file or ensure files exist in Stage J directories."
                    ))
                    return
                
                self.stage_v_process_btn.configure(
                    state="disabled",
                    text="Processing Batch..."
                )
                
                # Get prompts
                prompt_1 = self.stage_v_prompt1_text.get("1.0", tk.END).strip()
                prompt_2 = self.stage_v_prompt2_text.get("1.0", tk.END).strip()
                
                if not prompt_1:
                    default_prompt_1 = self.prompt_manager.get_prompt("Test Bank Generation - Step 1 Prompt")
                    if default_prompt_1:
                        prompt_1 = default_prompt_1
                        self.logger.info("Using default Step 1 prompt from prompts.json")
                    else:
                        self.root.after(0, lambda: messagebox.showerror("Error", "Please enter a prompt for Step 1"))
                        return
                
                if not prompt_2:
                    default_prompt_2 = self.prompt_manager.get_prompt("Test Bank Generation - Step 2 Prompt")
                    if default_prompt_2:
                        prompt_2 = default_prompt_2
                        self.logger.info("Using default Step 2 prompt from prompts.json")
                    else:
                        self.root.after(0, lambda: messagebox.showerror("Error", "Please enter a prompt for Step 2"))
                        return
                
                # Get models
                model_1 = self.stage_v_model1_var.get() if hasattr(self, 'stage_v_model1_var') else "gemini-2.5-pro"
                model_2 = self.stage_v_model2_var.get() if hasattr(self, 'stage_v_model2_var') else "gemini-2.5-pro"
                
                # Get delay
                try:
                    delay_seconds = float(self.stage_v_delay_var.get() if hasattr(self, 'stage_v_delay_var') else "5")
                    if delay_seconds < 0:
                        delay_seconds = 0
                except:
                    delay_seconds = 5
                
                total_pairs = len(valid_pairs)
                completed = 0
                failed = 0
                
                # Reset progress bar
                self.root.after(0, lambda: self.stage_v_progress_bar.set(0))
                
                # Process each pair
                for idx, pair in enumerate(valid_pairs):
                    stage_j_path = pair['stage_j_path']
                    word_path = pair['word_path']
                    ocr_extraction_path = pair.get('ocr_extraction_path')
                    
                    stage_j_name = os.path.basename(stage_j_path)
                    word_name = os.path.basename(word_path)
                    
                    # Update status to processing
                    if hasattr(self, 'stage_v_pairs_info_list'):
                        for pair_info in self.stage_v_pairs_info_list:
                            if pair_info['pair'] == pair:
                                self.root.after(0, lambda sl=pair_info['status_label']:
                                               sl.configure(text="PROCESSING", text_color="blue"))
                                break
                    
                    # Update progress label
                    self.root.after(0, lambda idx=idx, total=total_pairs, sj=stage_j_name, w=word_name:
                                   self.stage_v_status_label.configure(
                                       text=f"Processing pair {idx+1}/{total}: {sj} ↔ {w}"))
                    
                    # Progress bar
                    progress = idx / total_pairs
                    self.root.after(0, lambda p=progress: self.stage_v_progress_bar.set(p))
                    
                    try:
                        def progress_callback(msg: str):
                            self.root.after(0, lambda m=msg:
                                           self.stage_v_status_label.configure(text=m))
                        
                        # Process Stage V
                        output_path = self.stage_v_processor.process_stage_v(
                            stage_j_path=stage_j_path,
                            word_file_path=word_path,
                            ocr_extraction_json_path=ocr_extraction_path,
                            prompt_1=prompt_1,
                            model_name_1=model_1,
                            prompt_2=prompt_2,
                            model_name_2=model_2,
                            output_dir=self.get_default_output_dir(stage_j_path),
                            progress_callback=progress_callback
                        )
                        
                        if output_path and os.path.exists(output_path):
                            completed += 1
                            pair['output_path'] = output_path
                            pair['status'] = 'completed'
                            
                            # Update status to completed
                            if hasattr(self, 'stage_v_pairs_info_list'):
                                for pair_info in self.stage_v_pairs_info_list:
                                    if pair_info['pair'] == pair:
                                        self.root.after(0, lambda sl=pair_info['status_label']:
                                                       sl.configure(text="COMPLETED", text_color="green"))
                                        break
                            
                            self.logger.info(
                                f"Successfully processed: {stage_j_name} ↔ {word_name} -> {os.path.basename(output_path)}"
                            )
                            
                            # Update last_stage_v_path to the last completed output
                            self.last_stage_v_path = output_path
                        else:
                            failed += 1
                            pair['status'] = 'failed'
                            pair['error'] = "Processing returned no output"
                            
                            # Update status to failed
                            if hasattr(self, 'stage_v_pairs_info_list'):
                                for pair_info in self.stage_v_pairs_info_list:
                                    if pair_info['pair'] == pair:
                                        self.root.after(0, lambda sl=pair_info['status_label']:
                                                       sl.configure(text="FAILED", text_color="red"))
                                        break
                            
                            self.logger.error(f"Failed to process: {stage_j_name} ↔ {word_name}")
                            
                    except Exception as e:
                        failed += 1
                        pair['status'] = 'failed'
                        pair['error'] = str(e)
                        self.logger.error(f"Error processing {stage_j_name} ↔ {word_name}: {str(e)}", exc_info=True)
                        
                        # Update status to failed
                        if hasattr(self, 'stage_v_pairs_info_list'):
                            for pair_info in self.stage_v_pairs_info_list:
                                if pair_info['pair'] == pair:
                                    self.root.after(0, lambda sl=pair_info['status_label']:
                                                   sl.configure(text="FAILED", text_color="red"))
                                    break
                    
                    # Delay before next batch (except for the last one)
                    if idx < total_pairs - 1 and delay_seconds > 0:
                        self.root.after(0, lambda: self.stage_v_status_label.configure(
                            text=f"Waiting {delay_seconds} seconds before next batch..."))
                        import time
                        time.sleep(delay_seconds)
                
                # Final progress update
                self.root.after(0, lambda: self.stage_v_progress_bar.set(1.0))
                
                self.root.after(0, lambda: self.stage_v_status_label.configure(
                    text=f"Batch completed: {completed} succeeded, {failed} failed",
                    text_color="green" if failed == 0 else "orange"
                ))
                
                # Show summary
                self.root.after(0, lambda: messagebox.showinfo(
                    "Batch Processing Complete",
                    f"Stage V batch completed!\n\n"
                    f"Total pairs: {total_pairs}\n"
                    f"Successful: {completed}\n"
                    f"Failed: {failed}"
                ))
                
                # Enable view CSV button if at least one succeeded
                if completed > 0:
                    self.root.after(0, lambda: self.stage_v_view_csv_btn.configure(state="normal"))
            
            except Exception as e:
                self.logger.error(f"Error in batch Stage V processing: {str(e)}", exc_info=True)
                self.root.after(0, lambda: messagebox.showerror(
                    "Error",
                    f"Batch processing error:\n{str(e)}"
                ))
            finally:
                self.root.after(0, lambda: self.stage_v_process_btn.configure(
                    state="normal",
                    text="Process All Pairs"
                ))
        
        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def process_stage_v(self):
        """Process Stage V (redirects to batch processing)"""
        # Redirect to batch processing
        self.process_stage_v_batch()
    
    def setup_stage_m_ui(self, parent):
        """Setup UI for Stage M: Topic List Extraction (Batch Processing)"""
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
        
        title = ctk.CTkLabel(main_frame, text="Topic List Extraction", 
                            font=ctk.CTkFont(size=24, weight="bold"))
        title.pack(pady=(0, 20))
        
        desc = ctk.CTkLabel(main_frame, text="Extract unique chapter / subchapter / topic combinations from Flashcard files (ac) - Batch Processing.", 
                           font=ctk.CTkFont(size=12), text_color="gray")
        desc.pack(pady=(0, 20))

        # Flashcard Files Selection - Batch Processing
        stage_h_frame = ctk.CTkFrame(main_frame)
        stage_h_frame.pack(fill="x", pady=10)

        ctk.CTkLabel(
            stage_h_frame,
            text="Flashcard JSON Files (ac files) - Batch Processing",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=5)

        # File selection buttons
        buttons_frame_h = ctk.CTkFrame(stage_h_frame)
        buttons_frame_h.pack(fill="x", padx=10, pady=5)
        
        def browse_multiple_stage_h_files():
            filenames = filedialog.askopenfilenames(
                title="Select Flashcard JSON files",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            if filenames:
                if not hasattr(self, 'stage_m_selected_stage_h_files'):
                    self.stage_m_selected_stage_h_files = []
                for filename in filenames:
                    if filename not in self.stage_m_selected_stage_h_files:
                        self.stage_m_selected_stage_h_files.append(filename)
                        self._add_stage_m_stage_h_file_to_ui(filename)
        
        def select_folder_stage_h_files():
            folder_path = filedialog.askdirectory(
                title="Select folder containing Flashcard JSON files"
            )
            if not folder_path:
                return
            
            import glob
            json_files = glob.glob(os.path.join(folder_path, "*.json"))
            json_files = [f for f in json_files if os.path.isfile(f)]
            
            if not json_files:
                messagebox.showinfo("Info", "No JSON files found in selected folder")
                return
            
            if not hasattr(self, 'stage_m_selected_stage_h_files'):
                self.stage_m_selected_stage_h_files = []
            
            added_count = 0
            for json_file in json_files:
                if json_file not in self.stage_m_selected_stage_h_files:
                    self.stage_m_selected_stage_h_files.append(json_file)
                    self._add_stage_m_stage_h_file_to_ui(json_file)
                    added_count += 1
            
            if added_count > 0:
                messagebox.showinfo("Success", f"Added {added_count} JSON file(s) from folder")
        
        ctk.CTkButton(
            buttons_frame_h,
            text="Browse Multiple Flashcard Files",
            command=browse_multiple_stage_h_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        ctk.CTkButton(
            buttons_frame_h,
            text="Select Folder (Flashcard)",
            command=select_folder_stage_h_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)

        # Flashcard files list (scrollable)
        list_frame_h = ctk.CTkFrame(stage_h_frame)
        list_frame_h.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            list_frame_h,
            text="Selected Flashcard Files:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_m_stage_h_files_list_scroll'):
            self.stage_m_stage_h_files_list_scroll = ctk.CTkScrollableFrame(list_frame_h, height=150)
        self.stage_m_stage_h_files_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize selected files list
        if not hasattr(self, 'stage_m_selected_stage_h_files'):
            self.stage_m_selected_stage_h_files = []
        
        # Delay Setting
        delay_frame = ctk.CTkFrame(main_frame)
        delay_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(delay_frame, text="Delay Between Files (seconds):", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_m_delay_var'):
            self.stage_m_delay_var = ctk.StringVar(value="5")
        
        delay_entry_frame = ctk.CTkFrame(delay_frame)
        delay_entry_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(delay_entry_frame, text="Delay:").pack(side="left", padx=5)
        delay_entry = ctk.CTkEntry(delay_entry_frame, textvariable=self.stage_m_delay_var, width=100)
        delay_entry.pack(side="left", padx=5)
        ctk.CTkLabel(delay_entry_frame, text="seconds", text_color="gray").pack(side="left", padx=5)

        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)

        if not hasattr(self, 'stage_m_process_btn'):
            self.stage_m_process_btn = ctk.CTkButton(
                process_btn_frame,
                text="Process All Files",
                command=self.process_stage_m_batch,
                font=ctk.CTkFont(size=16, weight="bold"),
                height=40
            )
        self.stage_m_process_btn.pack(pady=10)
        
        # Progress bar for batch processing
        if not hasattr(self, 'stage_m_progress_bar'):
            self.stage_m_progress_bar = ctk.CTkProgressBar(process_btn_frame, width=400)
        self.stage_m_progress_bar.pack(pady=10)
        self.stage_m_progress_bar.set(0)

        # View CSV button
        if not hasattr(self, 'stage_m_view_csv_btn'):
            self.stage_m_view_csv_btn = ctk.CTkButton(
                process_btn_frame,
                text="View CSV",
                command=self.view_csv_stage_m,
                width=150,
                height=40,
                font=ctk.CTkFont(size=14),
                fg_color="green",
                hover_color="darkgreen",
                state="disabled"
            )
        self.stage_m_view_csv_btn.pack(pady=10)

        # Status Label
        if not hasattr(self, 'stage_m_status_label'):
            self.stage_m_status_label = ctk.CTkLabel(
                process_btn_frame,
                text="Ready",
                font=ctk.CTkFont(size=12),
                text_color="gray"
            )
        self.stage_m_status_label.pack(pady=5)

    def validate_stage_m_files(self):
        """Validate Stage M input files (kept for backward compatibility)"""
        # Validation is now done per file in batch mode
        pass

    def _add_stage_m_stage_h_file_to_ui(self, file_path: str):
        """Add a Flashcard file to the UI list"""
        file_frame = ctk.CTkFrame(self.stage_m_stage_h_files_list_scroll)
        file_frame.pack(fill="x", padx=5, pady=2)
        
        file_name = os.path.basename(file_path)
        
        name_label = ctk.CTkLabel(
            file_frame,
            text=file_name,
            font=ctk.CTkFont(size=11),
            anchor="w"
        )
        name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
        
        def remove_file():
            if file_path in self.stage_m_selected_stage_h_files:
                self.stage_m_selected_stage_h_files.remove(file_path)
            file_frame.destroy()
        
        remove_btn = ctk.CTkButton(
            file_frame,
            text="✕",
            command=remove_file,
            width=30,
            height=25,
            fg_color="red",
            hover_color="darkred"
        )
        remove_btn.pack(side="right", padx=5, pady=5)

    def process_stage_m_batch(self):
        """Process multiple Flashcard files for Stage M (Batch Processing)"""
        def worker():
            try:
                if not hasattr(self, 'stage_m_selected_stage_h_files') or not self.stage_m_selected_stage_h_files:
                    self.root.after(0, lambda: messagebox.showwarning(
                        "Warning",
                        "Please add at least one Flashcard JSON file"
                    ))
                    return
                
                # Filter valid files
                valid_files = [f for f in self.stage_m_selected_stage_h_files if os.path.exists(f)]
                
                if not valid_files:
                    self.root.after(0, lambda: messagebox.showwarning(
                        "Warning",
                        "No valid files found. Please check file paths."
                    ))
                    return
                
                self.stage_m_process_btn.configure(
                    state="disabled",
                    text="Processing Batch..."
                )
                
                # Get delay
                try:
                    delay_seconds = float(self.stage_m_delay_var.get() if hasattr(self, 'stage_m_delay_var') else "5")
                    if delay_seconds < 0:
                        delay_seconds = 0
                except:
                    delay_seconds = 5
                
                total_files = len(valid_files)
                completed = 0
                failed = 0
                output_paths = []
                
                # Reset progress bar
                self.root.after(0, lambda: self.stage_m_progress_bar.set(0))
                
                # Process each file sequentially
                for idx, stage_h_path in enumerate(valid_files):
                    file_name = os.path.basename(stage_h_path)
                    
                    # Update progress label
                    self.root.after(0, lambda idx=idx, total=total_files, fn=file_name:
                                   self.stage_m_status_label.configure(
                                       text=f"Processing file {idx+1}/{total}: {fn}",
                                       text_color="blue"
                                   ))
                    
                    # Progress bar
                    progress = idx / total_files
                    self.root.after(0, lambda p=progress: self.stage_m_progress_bar.set(p))
                    
                    try:
                        def progress_callback(msg: str):
                            self.root.after(0, lambda m=msg:
                                           self.stage_m_status_label.configure(text=m))
                        
                        # Process Stage M
                        output_path = self.stage_m_processor.process_stage_m(
                            stage_h_path=stage_h_path,
                            output_dir=self.get_default_output_dir(stage_h_path),
                            progress_callback=progress_callback
                        )
                        
                        if output_path and os.path.exists(output_path):
                            completed += 1
                            output_paths.append(output_path)
                            self.last_stage_m_path = output_path  # Keep last output for CSV view
                            
                            self.logger.info(
                                f"Successfully processed: {file_name} -> {os.path.basename(output_path)}"
                            )
                        else:
                            failed += 1
                            self.logger.error(f"Failed to process: {file_name}")
                            
                    except Exception as e:
                        failed += 1
                        self.logger.error(f"Error processing {file_name}: {str(e)}", exc_info=True)
                    
                    # Delay before next file (except for the last one)
                    if idx < total_files - 1 and delay_seconds > 0:
                        self.root.after(0, lambda: self.stage_m_status_label.configure(
                            text=f"Waiting {delay_seconds} seconds before next file..."))
                        import time
                        time.sleep(delay_seconds)
                
                # Final progress update
                self.root.after(0, lambda: self.stage_m_progress_bar.set(1.0))
                
                self.root.after(0, lambda: self.stage_m_status_label.configure(
                    text=f"Batch completed: {completed} succeeded, {failed} failed",
                    text_color="green" if failed == 0 else "orange"
                ))
                
                # Show summary
                self.root.after(0, lambda: messagebox.showinfo(
                    "Batch Processing Complete",
                    f"Stage M batch completed!\n\n"
                    f"Total files: {total_files}\n"
                    f"Successful: {completed}\n"
                    f"Failed: {failed}"
                ))
                
                # Enable view CSV button if at least one succeeded
                if completed > 0:
                    self.root.after(0, lambda: self.stage_m_view_csv_btn.configure(state="normal"))
            
            except Exception as e:
                self.logger.error(f"Error in batch Stage M processing: {str(e)}", exc_info=True)
                self.root.after(0, lambda: messagebox.showerror(
                    "Error",
                    f"Batch processing error:\n{str(e)}"
                ))
            finally:
                self.root.after(0, lambda: self.stage_m_process_btn.configure(
                    state="normal",
                    text="Process All Files"
                ))
        
        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()

    def process_stage_m(self):
        """Process Stage M (redirects to batch processing for backward compatibility)"""
        # Redirect to batch processing
        self.process_stage_m_batch()
    
    def setup_stage_l_ui(self, parent):
        """Setup UI for Stage L: Chapter Overview (Batch Processing)"""
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
        
        title = ctk.CTkLabel(main_frame, text="Chapter Summary", 
                    font=ctk.CTkFont(size=24, weight="bold"))
        title.pack(pady=(0, 20))
        
        desc = ctk.CTkLabel(
            main_frame,
            text="Generate per-topic chapter overview from Tagged data (a file) and Test Bank (b file) - Batch Processing.",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        desc.pack(pady=(0, 20))

        # Stage J Files Selection - Batch Processing
        stage_j_frame = ctk.CTkFrame(main_frame)
        stage_j_frame.pack(fill="x", pady=10)

        ctk.CTkLabel(
            stage_j_frame,
            text="Tagged Data JSON Files (a files) - Batch Processing",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=5)

        # File selection buttons for Stage J
        buttons_frame_j = ctk.CTkFrame(stage_j_frame)
        buttons_frame_j.pack(fill="x", padx=10, pady=5)
        
        def browse_multiple_stage_j_files():
            filenames = filedialog.askopenfilenames(
                title="Select Tagged Data JSON files",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            if filenames:
                if not hasattr(self, 'stage_l_selected_stage_j_files'):
                    self.stage_l_selected_stage_j_files = []
                for filename in filenames:
                    if filename not in self.stage_l_selected_stage_j_files:
                        self.stage_l_selected_stage_j_files.append(filename)
                        self._add_stage_l_stage_j_file_to_ui(filename)
                self._update_stage_l_pairs()
        
        def select_folder_stage_j_files():
            folder_path = filedialog.askdirectory(
                title="Select folder containing Tagged Data JSON files"
            )
            if not folder_path:
                return
            
            import glob
            json_files = glob.glob(os.path.join(folder_path, "*.json"))
            json_files = [f for f in json_files if os.path.isfile(f)]
            
            if not json_files:
                messagebox.showinfo("Info", "No JSON files found in selected folder")
                return
            
            if not hasattr(self, 'stage_l_selected_stage_j_files'):
                self.stage_l_selected_stage_j_files = []
            
            added_count = 0
            for json_file in json_files:
                if json_file not in self.stage_l_selected_stage_j_files:
                    self.stage_l_selected_stage_j_files.append(json_file)
                    self._add_stage_l_stage_j_file_to_ui(json_file)
                    added_count += 1
            
            if added_count > 0:
                self._update_stage_l_pairs()
            messagebox.showinfo("Success", f"Added {added_count} JSON file(s) from folder")
        
        ctk.CTkButton(
            buttons_frame_j,
            text="Browse Multiple Stage J Files",
            command=browse_multiple_stage_j_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        ctk.CTkButton(
            buttons_frame_j,
            text="Select Folder (Stage J)",
            command=select_folder_stage_j_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)

        # Stage J files list (scrollable)
        list_frame_j = ctk.CTkFrame(stage_j_frame)
        list_frame_j.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            list_frame_j,
            text="Selected Stage J Files:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_l_stage_j_files_list_scroll'):
            self.stage_l_stage_j_files_list_scroll = ctk.CTkScrollableFrame(list_frame_j, height=100)
        self.stage_l_stage_j_files_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize selected files list
        if not hasattr(self, 'stage_l_selected_stage_j_files'):
            self.stage_l_selected_stage_j_files = []

        # Stage V Files Selection - Batch Processing
        stage_v_frame = ctk.CTkFrame(main_frame)
        stage_v_frame.pack(fill="x", pady=10)

        ctk.CTkLabel(
            stage_v_frame,
            text="Test Bank JSON Files (b files) - Batch Processing",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=5)

        # File selection buttons for Stage V
        buttons_frame_v = ctk.CTkFrame(stage_v_frame)
        buttons_frame_v.pack(fill="x", padx=10, pady=5)
        
        def browse_multiple_stage_v_files():
            filenames = filedialog.askopenfilenames(
                title="Select Test Bank JSON files",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            if filenames:
                if not hasattr(self, 'stage_l_selected_stage_v_files'):
                    self.stage_l_selected_stage_v_files = []
                for filename in filenames:
                    if filename not in self.stage_l_selected_stage_v_files:
                        self.stage_l_selected_stage_v_files.append(filename)
                        self._add_stage_l_stage_v_file_to_ui(filename)
                self._update_stage_l_pairs()
        
        def select_folder_stage_v_files():
            folder_path = filedialog.askdirectory(
                title="Select folder containing Test Bank JSON files"
            )
            if not folder_path:
                return
            
            import glob
            json_files = glob.glob(os.path.join(folder_path, "*.json"))
            json_files = [f for f in json_files if os.path.isfile(f)]
            
            if not json_files:
                messagebox.showinfo("Info", "No JSON files found in selected folder")
                return
            
            if not hasattr(self, 'stage_l_selected_stage_v_files'):
                self.stage_l_selected_stage_v_files = []
            
            added_count = 0
            for json_file in json_files:
                if json_file not in self.stage_l_selected_stage_v_files:
                    self.stage_l_selected_stage_v_files.append(json_file)
                    self._add_stage_l_stage_v_file_to_ui(json_file)
                    added_count += 1
            
            if added_count > 0:
                self._update_stage_l_pairs()
            messagebox.showinfo("Success", f"Added {added_count} JSON file(s) from folder")
        
        ctk.CTkButton(
            buttons_frame_v,
            text="Browse Multiple Stage V Files",
            command=browse_multiple_stage_v_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        ctk.CTkButton(
            buttons_frame_v,
            text="Select Folder (Stage V)",
            command=select_folder_stage_v_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)

        # Stage V files list (scrollable)
        list_frame_v = ctk.CTkFrame(stage_v_frame)
        list_frame_v.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            list_frame_v,
            text="Selected Stage V Files:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_l_stage_v_files_list_scroll'):
            self.stage_l_stage_v_files_list_scroll = ctk.CTkScrollableFrame(list_frame_v, height=100)
        self.stage_l_stage_v_files_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize selected files list
        if not hasattr(self, 'stage_l_selected_stage_v_files'):
            self.stage_l_selected_stage_v_files = []
        
        # Pairs Section
        pairs_frame = ctk.CTkFrame(main_frame)
        pairs_frame.pack(fill="x", pady=10)
        
        pairs_header_frame = ctk.CTkFrame(pairs_frame)
        pairs_header_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(
            pairs_header_frame,
            text="Pairs (Stage J ↔ Stage V):",
            font=ctk.CTkFont(size=14, weight="bold"),
        ).pack(side="left", padx=10, pady=5)
        
        ctk.CTkButton(
            pairs_header_frame,
            text="Auto-Pair",
            command=self._auto_pair_stage_l_files,
            width=120,
            height=30,
            fg_color="green",
            hover_color="darkgreen"
        ).pack(side="right", padx=10, pady=5)
        
        # Pairs list (scrollable)
        pairs_list_frame = ctk.CTkFrame(pairs_frame)
        pairs_list_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            pairs_list_frame,
            text="File Pairs:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_l_pairs_list_scroll'):
            self.stage_l_pairs_list_scroll = ctk.CTkScrollableFrame(pairs_list_frame, height=150)
        self.stage_l_pairs_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize pairs
        if not hasattr(self, 'stage_l_pairs'):
            self.stage_l_pairs = []
        if not hasattr(self, 'stage_l_pairs_info_list'):
            self.stage_l_pairs_info_list = []

        # Prompt Section
        prompt_frame = ctk.CTkFrame(main_frame)
        prompt_frame.pack(fill="x", pady=10)

        ctk.CTkLabel(
            prompt_frame,
            text="Prompt for Chapter Overview:",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=5)

        # Stage L prompt mode (default vs custom)
        stage_l_mode_frame = ctk.CTkFrame(prompt_frame)
        stage_l_mode_frame.pack(fill="x", padx=10, pady=(0, 5))
        if not hasattr(self, 'stage_l_prompt_type_var'):
            self.stage_l_prompt_type_var = ctk.StringVar(value="default")
        ctk.CTkRadioButton(
            stage_l_mode_frame,
            text="Use Default Prompt",
            variable=self.stage_l_prompt_type_var,
            value="default",
            command=self.on_stage_l_prompt_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)
        ctk.CTkRadioButton(
            stage_l_mode_frame,
            text="Use Custom Prompt",
            variable=self.stage_l_prompt_type_var,
            value="custom",
            command=self.on_stage_l_prompt_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)

        # Default Stage L prompt combobox (predefined prompts)
        stage_l_default_frame = ctk.CTkFrame(prompt_frame)
        stage_l_default_frame.pack(fill="x", padx=10, pady=(0, 10))

        ctk.CTkLabel(
            stage_l_default_frame,
            text="Default Chapter Summary Prompt:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", pady=(0, 5))

        stage_l_prompt_names = self.prompt_manager.get_prompt_names()
        preferred_stage_l_name = "Chapter Summary Prompt"
        if preferred_stage_l_name in stage_l_prompt_names:
            stage_l_default_value = preferred_stage_l_name
        else:
            stage_l_default_value = stage_l_prompt_names[0] if stage_l_prompt_names else ""
        if not hasattr(self, 'stage_l_default_prompt_var'):
            self.stage_l_default_prompt_var = ctk.StringVar(value=stage_l_default_value)
        if not hasattr(self, 'stage_l_default_prompt_combo'):
            self.stage_l_default_prompt_combo = ctk.CTkComboBox(
                stage_l_default_frame,
                values=stage_l_prompt_names,
                variable=self.stage_l_default_prompt_var,
                width=400,
                command=self.on_stage_l_default_prompt_selected,
            )
        self.stage_l_default_prompt_combo.pack(anchor="w", pady=(0, 5))

        # Textbox for Stage L prompt (default-filled or custom)
        if not hasattr(self, "stage_l_prompt_text"):
            self.stage_l_prompt_text = ctk.CTkTextbox(prompt_frame, height=150, font=self.farsi_text_font)
        self.stage_l_prompt_text.pack(fill="x", padx=10, pady=(0, 5))
        
        # Initialize prompt textbox with default prompt
        self.on_stage_l_prompt_type_change()

        # Model Selection
        model_frame = ctk.CTkFrame(main_frame)
        model_frame.pack(fill="x", pady=10)

        ctk.CTkLabel(
            model_frame,
            text="Model for Chapter Summary:",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=5)

        default_model = "gemini-2.5-pro"
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
        
        # Delay Setting
        delay_frame = ctk.CTkFrame(main_frame)
        delay_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(delay_frame, text="Delay Between Pairs (seconds):", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_l_delay_var'):
            self.stage_l_delay_var = ctk.StringVar(value="5")
        
        delay_entry_frame = ctk.CTkFrame(delay_frame)
        delay_entry_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(delay_entry_frame, text="Delay:").pack(side="left", padx=5)
        delay_entry = ctk.CTkEntry(delay_entry_frame, textvariable=self.stage_l_delay_var, width=100)
        delay_entry.pack(side="left", padx=5)
        ctk.CTkLabel(delay_entry_frame, text="seconds", text_color="gray").pack(side="left", padx=5)

        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)

        if not hasattr(self, 'stage_l_process_btn'):
            self.stage_l_process_btn = ctk.CTkButton(
                process_btn_frame,
                text="Process All Pairs",
                command=self.process_stage_l_batch,
                font=ctk.CTkFont(size=16, weight="bold"),
                height=40
            )
        self.stage_l_process_btn.pack(pady=10)
        
        # Progress bar for batch processing
        if not hasattr(self, 'stage_l_progress_bar'):
            self.stage_l_progress_bar = ctk.CTkProgressBar(process_btn_frame, width=400)
        self.stage_l_progress_bar.pack(pady=10)
        self.stage_l_progress_bar.set(0)

        # View CSV button
        if not hasattr(self, 'stage_l_view_csv_btn'):
            self.stage_l_view_csv_btn = ctk.CTkButton(
                process_btn_frame,
                text="View CSV",
                command=self.view_csv_stage_l,
                width=150,
                height=40,
                font=ctk.CTkFont(size=14),
                fg_color="green",
                hover_color="darkgreen",
                state="disabled"
            )
        self.stage_l_view_csv_btn.pack(pady=10)

        # Status Label
        if not hasattr(self, 'stage_l_status_label'):
            self.stage_l_status_label = ctk.CTkLabel(
                process_btn_frame,
                text="Ready",
                font=ctk.CTkFont(size=12),
                text_color="gray"
            )
        self.stage_l_status_label.pack(pady=5)

    def validate_stage_l_files(self):
        """Validate Stage L input files (kept for backward compatibility)"""
        # Validation is now done per pair in batch mode
        pass

    def _add_stage_l_stage_j_file_to_ui(self, file_path: str):
        """Add a Stage J file to the UI list"""
        file_frame = ctk.CTkFrame(self.stage_l_stage_j_files_list_scroll)
        file_frame.pack(fill="x", padx=5, pady=2)
        
        file_name = os.path.basename(file_path)
        
        name_label = ctk.CTkLabel(
            file_frame,
            text=file_name,
            font=ctk.CTkFont(size=11),
            anchor="w"
        )
        name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
        
        def remove_file():
            if file_path in self.stage_l_selected_stage_j_files:
                self.stage_l_selected_stage_j_files.remove(file_path)
            file_frame.destroy()
            self._update_stage_l_pairs()
        
        remove_btn = ctk.CTkButton(
            file_frame,
            text="✕",
            command=remove_file,
            width=30,
            height=25,
            fg_color="red",
            hover_color="darkred"
        )
        remove_btn.pack(side="right", padx=5, pady=5)
    
    def _add_stage_l_stage_v_file_to_ui(self, file_path: str):
        """Add a Stage V file to the UI list"""
        file_frame = ctk.CTkFrame(self.stage_l_stage_v_files_list_scroll)
        file_frame.pack(fill="x", padx=5, pady=2)
        
        file_name = os.path.basename(file_path)
        
        name_label = ctk.CTkLabel(
            file_frame,
            text=file_name,
            font=ctk.CTkFont(size=11),
            anchor="w"
        )
        name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
        
        def remove_file():
            if file_path in self.stage_l_selected_stage_v_files:
                self.stage_l_selected_stage_v_files.remove(file_path)
            file_frame.destroy()
            self._update_stage_l_pairs()
        
        remove_btn = ctk.CTkButton(
            file_frame,
            text="✕",
            command=remove_file,
            width=30,
            height=25,
            fg_color="red",
            hover_color="darkred"
        )
        remove_btn.pack(side="right", padx=5, pady=5)
    
    def _extract_book_chapter_from_stage_j_for_l(self, stage_j_path: str):
        """Extract book and chapter from Stage J file (from PointId or filename)"""
        try:
            # Try to load Stage J and extract from PointId
            data = json.load(open(stage_j_path, 'r', encoding='utf-8'))
            records = data.get("data") or data.get("rows", [])
            if records and records[0].get("PointId"):
                point_id = records[0].get("PointId")
                if isinstance(point_id, str) and len(point_id) >= 6:
                    book_id = int(point_id[0:3])
                    chapter_id = int(point_id[3:6])
                    return book_id, chapter_id
        except:
            pass
        
        # Fallback: try to extract from filename (a{book}{chapter}.json or a{book}{chapter}+{name}.json)
        try:
            basename = os.path.basename(stage_j_path)
            name_without_ext = os.path.splitext(basename)[0]
            import re
            # Try pattern: a{book}{chapter}+{name}
            match = re.match(r'^a(\d{3})(\d{3})\+', name_without_ext)
            if match:
                book_id = int(match.group(1))
                chapter_id = int(match.group(2))
                return book_id, chapter_id
            # Try pattern: a{book}{chapter}
            if name_without_ext.startswith('a') and len(name_without_ext) >= 7:
                book_chapter = name_without_ext[1:]
                book_id = int(book_chapter[0:3])
                chapter_id = int(book_chapter[3:6])
                return book_id, chapter_id
        except:
            pass
        
        return None, None
    
    def _extract_book_chapter_from_stage_v_for_l(self, stage_v_path: str):
        """Extract book and chapter from Stage V file (from PointId or filename)"""
        try:
            # Try to load Stage V and extract from PointId
            data = json.load(open(stage_v_path, 'r', encoding='utf-8'))
            records = data.get("data") or data.get("rows", [])
            if records and records[0].get("PointId"):
                point_id = records[0].get("PointId")
                if isinstance(point_id, str) and len(point_id) >= 6:
                    book_id = int(point_id[0:3])
                    chapter_id = int(point_id[3:6])
                    return book_id, chapter_id
        except:
            pass
        
        # Fallback: try to extract from filename (b{book}{chapter}.json or b{book}{chapter}+{name}.json)
        try:
            basename = os.path.basename(stage_v_path)
            name_without_ext = os.path.splitext(basename)[0]
            import re
            # Try pattern: b{book}{chapter}+{name}
            match = re.match(r'^b(\d{3})(\d{3})\+', name_without_ext)
            if match:
                book_id = int(match.group(1))
                chapter_id = int(match.group(2))
                return book_id, chapter_id
            # Try pattern: b{book}{chapter}
            if name_without_ext.startswith('b') and len(name_without_ext) >= 7:
                book_chapter = name_without_ext[1:]
                book_id = int(book_chapter[0:3])
                chapter_id = int(book_chapter[3:6])
                return book_id, chapter_id
        except:
            pass
        
        return None, None
    
    def _auto_pair_stage_l_files(self):
        """Auto-pair Stage J files with Stage V files based on Book/Chapter"""
        if not hasattr(self, 'stage_l_selected_stage_j_files') or not self.stage_l_selected_stage_j_files:
            messagebox.showwarning("Warning", "Please add at least one Stage J file")
            return
        
        if not hasattr(self, 'stage_l_selected_stage_v_files') or not self.stage_l_selected_stage_v_files:
            messagebox.showwarning("Warning", "Please add at least one Stage V file")
            return
        
        pairs = []
        paired_stage_v_files = set()
        
        for stage_j_path in self.stage_l_selected_stage_j_files:
            book_id, chapter_id = self._extract_book_chapter_from_stage_j_for_l(stage_j_path)
            
            if book_id is None or chapter_id is None:
                # Can't extract book/chapter, skip
                continue
            
            # Find matching Stage V file
            matched_stage_v = None
            for stage_v_path in self.stage_l_selected_stage_v_files:
                if stage_v_path in paired_stage_v_files:
                    continue
                
                stage_v_book, stage_v_chapter = self._extract_book_chapter_from_stage_v_for_l(stage_v_path)
                
                if stage_v_book == book_id and stage_v_chapter == chapter_id:
                    matched_stage_v = stage_v_path
                    paired_stage_v_files.add(stage_v_path)
                    break
            
            pair = {
                'stage_j_path': stage_j_path,
                'stage_v_path': matched_stage_v,
                'book_id': book_id,
                'chapter_id': chapter_id,
                'status': 'pending',
                'output_path': None,
                'error': None
            }
            pairs.append(pair)
        
        self.stage_l_pairs = pairs
        self._update_stage_l_pairs_ui()
        
        paired_count = sum(1 for p in pairs if p['stage_v_path'] is not None)
        unpaired_count = len(pairs) - paired_count
        
        messagebox.showinfo(
            "Auto-Pairing Complete",
            f"Paired: {paired_count}\nUnpaired: {unpaired_count}"
        )
    
    def _update_stage_l_pairs(self):
        """Update pairs when files are added/removed"""
        # Clear existing pairs UI
        for widget in self.stage_l_pairs_list_scroll.winfo_children():
            widget.destroy()
        
        self.stage_l_pairs_info_list = []
        
        # Re-pair if we have pairs
        if hasattr(self, 'stage_l_pairs') and self.stage_l_pairs:
            self._update_stage_l_pairs_ui()
    
    def _update_stage_l_pairs_ui(self):
        """Update the pairs UI display"""
        # Clear existing pairs UI
        for widget in self.stage_l_pairs_list_scroll.winfo_children():
            widget.destroy()
        
        self.stage_l_pairs_info_list = []
        
        if not hasattr(self, 'stage_l_pairs') or not self.stage_l_pairs:
            return
        
        for pair in self.stage_l_pairs:
            pair_frame = ctk.CTkFrame(self.stage_l_pairs_list_scroll)
            pair_frame.pack(fill="x", padx=5, pady=2)
            
            stage_j_name = os.path.basename(pair['stage_j_path']) if pair['stage_j_path'] else "None"
            stage_v_name = os.path.basename(pair['stage_v_path']) if pair['stage_v_path'] else "None"
            
            pair_text = f"{stage_j_name} ↔ {stage_v_name}"
            
            # Main pair info
            info_frame = ctk.CTkFrame(pair_frame)
            info_frame.pack(fill="x", padx=5, pady=2)
            
            name_label = ctk.CTkLabel(
                info_frame,
                text=pair_text,
                font=ctk.CTkFont(size=11),
                anchor="w"
            )
            name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
            
            status_label = ctk.CTkLabel(
                info_frame,
                text=pair.get('status', 'pending').upper(),
                font=ctk.CTkFont(size=10),
                text_color="gray" if pair.get('status') == 'pending' else 
                          "green" if pair.get('status') == 'completed' else "red"
            )
            status_label.pack(side="right", padx=10, pady=5)
            
            self.stage_l_pairs_info_list.append({
                'pair': pair,
                'status_label': status_label,
                'frame': pair_frame
            })

    def process_stage_l_batch(self):
        """Process multiple Stage J + Stage V pairs for Stage L"""
        def worker():
            try:
                if not hasattr(self, 'stage_l_pairs') or not self.stage_l_pairs:
                    self.root.after(0, lambda: messagebox.showwarning(
                        "Warning",
                        "Please add files and create pairs first. Click 'Auto-Pair' button."
                    ))
                    return
                
                # Filter pairs that have both Stage J and Stage V file
                valid_pairs = [p for p in self.stage_l_pairs if p['stage_j_path'] and p['stage_v_path']]
                
                if not valid_pairs:
                    self.root.after(0, lambda: messagebox.showwarning(
                        "Warning",
                        "No valid pairs found. Each pair must have both Stage J and Stage V file."
                    ))
                    return
                
                self.stage_l_process_btn.configure(
                    state="disabled",
                    text="Processing Batch..."
                )
                
                # Get prompt
                prompt = self.stage_l_prompt_text.get("1.0", tk.END).strip()
                if not prompt:
                    default_prompt = self.prompt_manager.get_prompt("Chapter Summary Prompt")
                    if default_prompt:
                        prompt = default_prompt
                        self.logger.info("Using default Chapter Summary prompt from prompts.json")
                    else:
                        self.root.after(0, lambda: messagebox.showerror("Error", "Please enter a prompt"))
                        return
                
                # Get model
                model_name = self.stage_l_model_var.get() if hasattr(self, 'stage_l_model_var') else "gemini-2.5-pro"
                
                # Get delay
                try:
                    delay_seconds = float(self.stage_l_delay_var.get() if hasattr(self, 'stage_l_delay_var') else "5")
                    if delay_seconds < 0:
                        delay_seconds = 0
                except:
                    delay_seconds = 5
                
                total_pairs = len(valid_pairs)
                completed = 0
                failed = 0
                
                # Reset progress bar
                self.root.after(0, lambda: self.stage_l_progress_bar.set(0))
                
                # Process each pair
                for idx, pair in enumerate(valid_pairs):
                    stage_j_path = pair['stage_j_path']
                    stage_v_path = pair['stage_v_path']
                    
                    stage_j_name = os.path.basename(stage_j_path)
                    stage_v_name = os.path.basename(stage_v_path)
                    
                    # Update status to processing
                    if hasattr(self, 'stage_l_pairs_info_list'):
                        for pair_info in self.stage_l_pairs_info_list:
                            if pair_info['pair'] == pair:
                                self.root.after(0, lambda sl=pair_info['status_label']:
                                               sl.configure(text="PROCESSING", text_color="blue"))
                                break
                    
                    # Update progress label
                    self.root.after(0, lambda idx=idx, total=total_pairs, sj=stage_j_name, sv=stage_v_name:
                                   self.stage_l_status_label.configure(
                                       text=f"Processing pair {idx+1}/{total}: {sj} ↔ {sv}"))
                    
                    # Progress bar
                    progress = idx / total_pairs
                    self.root.after(0, lambda p=progress: self.stage_l_progress_bar.set(p))
                    
                    try:
                        def progress_callback(msg: str):
                            self.root.after(0, lambda m=msg:
                                           self.stage_l_status_label.configure(text=m))
                        
                        # Process Stage L
                        output_path = self.stage_l_processor.process_stage_l(
                            stage_j_path=stage_j_path,
                            stage_v_path=stage_v_path,
                            prompt=prompt,
                            model_name=model_name,
                            output_dir=self.get_default_output_dir(stage_j_path),
                            progress_callback=progress_callback
                        )
                        
                        if output_path and os.path.exists(output_path):
                            completed += 1
                            pair['output_path'] = output_path
                            pair['status'] = 'completed'
                            
                            # Update status to completed
                            if hasattr(self, 'stage_l_pairs_info_list'):
                                for pair_info in self.stage_l_pairs_info_list:
                                    if pair_info['pair'] == pair:
                                        self.root.after(0, lambda sl=pair_info['status_label']:
                                                       sl.configure(text="COMPLETED", text_color="green"))
                                        break
                            
                            self.logger.info(
                                f"Successfully processed: {stage_j_name} ↔ {stage_v_name} -> {os.path.basename(output_path)}"
                            )
                            
                            # Update last_stage_l_path to the last completed output
                            self.last_stage_l_path = output_path
                        else:
                            failed += 1
                            pair['status'] = 'failed'
                            pair['error'] = "Processing returned no output"
                            
                            # Update status to failed
                            if hasattr(self, 'stage_l_pairs_info_list'):
                                for pair_info in self.stage_l_pairs_info_list:
                                    if pair_info['pair'] == pair:
                                        self.root.after(0, lambda sl=pair_info['status_label']:
                                                       sl.configure(text="FAILED", text_color="red"))
                                        break
                            
                            self.logger.error(f"Failed to process: {stage_j_name} ↔ {stage_v_name}")
                            
                    except Exception as e:
                        failed += 1
                        pair['status'] = 'failed'
                        pair['error'] = str(e)
                        self.logger.error(f"Error processing {stage_j_name} ↔ {stage_v_name}: {str(e)}", exc_info=True)
                        
                        # Update status to failed
                        if hasattr(self, 'stage_l_pairs_info_list'):
                            for pair_info in self.stage_l_pairs_info_list:
                                if pair_info['pair'] == pair:
                                    self.root.after(0, lambda sl=pair_info['status_label']:
                                                   sl.configure(text="FAILED", text_color="red"))
                                    break
                    
                    # Delay before next batch (except for the last one)
                    if idx < total_pairs - 1 and delay_seconds > 0:
                        self.root.after(0, lambda: self.stage_l_status_label.configure(
                            text=f"Waiting {delay_seconds} seconds before next batch..."))
                        import time
                        time.sleep(delay_seconds)
                
                # Final progress update
                self.root.after(0, lambda: self.stage_l_progress_bar.set(1.0))
                
                self.root.after(0, lambda: self.stage_l_status_label.configure(
                    text=f"Batch completed: {completed} succeeded, {failed} failed",
                    text_color="green" if failed == 0 else "orange"
                ))
                
                # Show summary
                self.root.after(0, lambda: messagebox.showinfo(
                    "Batch Processing Complete",
                    f"Stage L batch completed!\n\n"
                    f"Total pairs: {total_pairs}\n"
                    f"Successful: {completed}\n"
                    f"Failed: {failed}"
                ))
                
                # Enable view CSV button if at least one succeeded
                if completed > 0:
                    self.root.after(0, lambda: self.stage_l_view_csv_btn.configure(state="normal"))
            
            except Exception as e:
                self.logger.error(f"Error in batch Stage L processing: {str(e)}", exc_info=True)
                self.root.after(0, lambda: messagebox.showerror(
                    "Error",
                    f"Batch processing error:\n{str(e)}"
                ))
            finally:
                self.root.after(0, lambda: self.stage_l_process_btn.configure(
                    state="normal",
                    text="Process All Pairs"
                ))
        
        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()

    def process_stage_l(self):
        """Process Stage L (redirects to batch processing for backward compatibility)"""
        # Redirect to batch processing
        self.process_stage_l_batch()
    
    def setup_stage_x_ui(self, parent):
        """Setup UI for Stage X: Book Changes Detection"""
        main_frame = ctk.CTkScrollableFrame(parent)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Navigation button
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
        
        title = ctk.CTkLabel(main_frame, text="Book Changes Detection", 
                    font=ctk.CTkFont(size=24, weight="bold"))
        title.pack(pady=(0, 20))
        
        desc = ctk.CTkLabel(
            main_frame,
            text="Detect changes between old book PDF and current Stage A data - Batch Processing. Part 1: Extract PDF. Part 2: Detect changes.",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        desc.pack(pady=(0, 20))
        
        # Old Book PDF Files Selection - Batch Processing
        pdf_frame = ctk.CTkFrame(main_frame)
        pdf_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(
            pdf_frame,
            text="Old Book PDF Files - Batch Processing",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=5)
        
        # File selection buttons for PDF
        buttons_frame_pdf = ctk.CTkFrame(pdf_frame)
        buttons_frame_pdf.pack(fill="x", padx=10, pady=5)
        
        def browse_multiple_pdf_files():
            filenames = filedialog.askopenfilenames(
                title="Select Old Book PDF files",
                filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")]
            )
            if filenames:
                if not hasattr(self, 'stage_x_selected_pdf_files'):
                    self.stage_x_selected_pdf_files = []
                for filename in filenames:
                    if filename not in self.stage_x_selected_pdf_files:
                        self.stage_x_selected_pdf_files.append(filename)
                        self._add_stage_x_pdf_file_to_ui(filename)
                self._update_stage_x_pairs()
        
        def select_folder_pdf_files():
            folder_path = filedialog.askdirectory(
                title="Select folder containing PDF files"
            )
            if not folder_path:
                return
            
            import glob
            pdf_files = glob.glob(os.path.join(folder_path, "*.pdf"))
            pdf_files = [f for f in pdf_files if os.path.isfile(f)]
            
            if not pdf_files:
                messagebox.showinfo("Info", "No PDF files found in selected folder")
                return
            
            if not hasattr(self, 'stage_x_selected_pdf_files'):
                self.stage_x_selected_pdf_files = []
            
            added_count = 0
            for pdf_file in pdf_files:
                if pdf_file not in self.stage_x_selected_pdf_files:
                    self.stage_x_selected_pdf_files.append(pdf_file)
                    self._add_stage_x_pdf_file_to_ui(pdf_file)
                    added_count += 1
            
            if added_count > 0:
                self._update_stage_x_pairs()
            messagebox.showinfo("Success", f"Added {added_count} PDF file(s) from folder")
        
        ctk.CTkButton(
            buttons_frame_pdf,
            text="Browse Multiple PDF Files",
            command=browse_multiple_pdf_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        ctk.CTkButton(
            buttons_frame_pdf,
            text="Select Folder (PDF)",
            command=select_folder_pdf_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        # PDF files list (scrollable)
        list_frame_pdf = ctk.CTkFrame(pdf_frame)
        list_frame_pdf.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            list_frame_pdf,
            text="Selected PDF Files:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_x_pdf_files_list_scroll'):
            self.stage_x_pdf_files_list_scroll = ctk.CTkScrollableFrame(list_frame_pdf, height=100)
        self.stage_x_pdf_files_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize selected files list
        if not hasattr(self, 'stage_x_selected_pdf_files'):
            self.stage_x_selected_pdf_files = []
        
        # Part 1: PDF Extraction Settings
        part1_frame = ctk.CTkFrame(main_frame)
        part1_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(part1_frame, text="Part 1: PDF Extraction Settings",
                    font=ctk.CTkFont(size=16, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        # PDF Extraction Prompt Section
        pdf_prompt_frame = ctk.CTkFrame(part1_frame)
        pdf_prompt_frame.pack(fill="x", padx=10, pady=5)
        ctk.CTkLabel(pdf_prompt_frame, text="PDF Extraction Prompt:",
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=5, pady=5)
        
        # PDF Extraction Prompt mode (default vs custom)
        pdf_prompt_mode_frame = ctk.CTkFrame(pdf_prompt_frame)
        pdf_prompt_mode_frame.pack(fill="x", padx=5, pady=(0, 5))
        if not hasattr(self, 'stage_x_pdf_prompt_type_var'):
            self.stage_x_pdf_prompt_type_var = ctk.StringVar(value="default")
        ctk.CTkRadioButton(
            pdf_prompt_mode_frame,
            text="Use Default Prompt",
            variable=self.stage_x_pdf_prompt_type_var,
            value="default",
            command=self.on_stage_x_pdf_prompt_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)
        ctk.CTkRadioButton(
            pdf_prompt_mode_frame,
            text="Use Custom Prompt",
            variable=self.stage_x_pdf_prompt_type_var,
            value="custom",
            command=self.on_stage_x_pdf_prompt_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)
        
        # Default PDF Extraction Prompt combobox
        pdf_prompt_default_frame = ctk.CTkFrame(pdf_prompt_frame)
        pdf_prompt_default_frame.pack(fill="x", padx=5, pady=(0, 5))
        ctk.CTkLabel(
            pdf_prompt_default_frame,
            text="Default PDF Extraction Prompt:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", pady=(0, 5))
        
        pdf_prompt_names = self.prompt_manager.get_prompt_names()
        preferred_pdf_prompt_name = "OCR Extraction Prompt"
        if preferred_pdf_prompt_name in pdf_prompt_names:
            pdf_prompt_default_value = preferred_pdf_prompt_name
        else:
            pdf_prompt_default_value = pdf_prompt_names[0] if pdf_prompt_names else ""
        if not hasattr(self, 'stage_x_pdf_default_prompt_var'):
            self.stage_x_pdf_default_prompt_var = ctk.StringVar(value=pdf_prompt_default_value)
        if not hasattr(self, 'stage_x_pdf_default_prompt_combo'):
            self.stage_x_pdf_default_prompt_combo = ctk.CTkComboBox(
                pdf_prompt_default_frame,
                values=pdf_prompt_names,
                variable=self.stage_x_pdf_default_prompt_var,
                width=400,
                command=self.on_stage_x_pdf_default_prompt_selected,
            )
        self.stage_x_pdf_default_prompt_combo.pack(anchor="w", pady=(0, 5))
        
        # Textbox for PDF Extraction prompt
        if not hasattr(self, 'stage_x_pdf_prompt_text'):
            self.stage_x_pdf_prompt_text = ctk.CTkTextbox(pdf_prompt_frame, height=100, font=self.farsi_text_font)
        self.stage_x_pdf_prompt_text.pack(fill="x", padx=5, pady=(0, 5))
        
        # Initialize PDF Extraction prompt textbox
        self.on_stage_x_pdf_prompt_type_change()
        
        # PDF Extraction Model
        pdf_model_frame = ctk.CTkFrame(part1_frame)
        pdf_model_frame.pack(fill="x", padx=10, pady=5)
        ctk.CTkLabel(pdf_model_frame, text="PDF Extraction Model:", width=150).pack(side="left", padx=5)
        if not hasattr(self, 'stage_x_pdf_model_var'):
            default_model = self.model_var.get() if hasattr(self, 'model_var') else "gemini-2.5-pro"
            self.stage_x_pdf_model_var = ctk.StringVar(value=default_model)
        ctk.CTkComboBox(pdf_model_frame, values=APIConfig.TEXT_MODELS, variable=self.stage_x_pdf_model_var, width=300).pack(side="left", padx=5)
        
        # Stage A Files Selection - Batch Processing
        stage_a_frame = ctk.CTkFrame(main_frame)
        stage_a_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(
            stage_a_frame,
            text="Stage A JSON Files (a files) - Batch Processing",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=5)
        
        # File selection buttons for Stage A
        buttons_frame_a = ctk.CTkFrame(stage_a_frame)
        buttons_frame_a.pack(fill="x", padx=10, pady=5)
        
        def browse_multiple_stage_a_files():
            filenames = filedialog.askopenfilenames(
                title="Select Stage A JSON files",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            if filenames:
                if not hasattr(self, 'stage_x_selected_stage_a_files'):
                    self.stage_x_selected_stage_a_files = []
                for filename in filenames:
                    if filename not in self.stage_x_selected_stage_a_files:
                        self.stage_x_selected_stage_a_files.append(filename)
                        self._add_stage_x_stage_a_file_to_ui(filename)
                self._update_stage_x_pairs()
        
        def select_folder_stage_a_files():
            folder_path = filedialog.askdirectory(
                title="Select folder containing Stage A JSON files"
            )
            if not folder_path:
                return
            
            import glob
            json_files = glob.glob(os.path.join(folder_path, "*.json"))
            json_files = [f for f in json_files if os.path.isfile(f)]
            
            if not json_files:
                messagebox.showinfo("Info", "No JSON files found in selected folder")
                return
            
            if not hasattr(self, 'stage_x_selected_stage_a_files'):
                self.stage_x_selected_stage_a_files = []
            
            added_count = 0
            for json_file in json_files:
                if json_file not in self.stage_x_selected_stage_a_files:
                    self.stage_x_selected_stage_a_files.append(json_file)
                    self._add_stage_x_stage_a_file_to_ui(json_file)
                    added_count += 1
            
            if added_count > 0:
                self._update_stage_x_pairs()
            messagebox.showinfo("Success", f"Added {added_count} JSON file(s) from folder")
        
        ctk.CTkButton(
            buttons_frame_a,
            text="Browse Multiple Stage A Files",
            command=browse_multiple_stage_a_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        ctk.CTkButton(
            buttons_frame_a,
            text="Select Folder (Stage A)",
            command=select_folder_stage_a_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        # Stage A files list (scrollable)
        list_frame_a = ctk.CTkFrame(stage_a_frame)
        list_frame_a.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            list_frame_a,
            text="Selected Stage A Files:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_x_stage_a_files_list_scroll'):
            self.stage_x_stage_a_files_list_scroll = ctk.CTkScrollableFrame(list_frame_a, height=100)
        self.stage_x_stage_a_files_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize selected files list
        if not hasattr(self, 'stage_x_selected_stage_a_files'):
            self.stage_x_selected_stage_a_files = []
        
        # Pairs Section
        pairs_frame = ctk.CTkFrame(main_frame)
        pairs_frame.pack(fill="x", pady=10)
        
        pairs_header_frame = ctk.CTkFrame(pairs_frame)
        pairs_header_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(
            pairs_header_frame,
            text="Pairs (PDF ↔ Stage A):",
            font=ctk.CTkFont(size=14, weight="bold"),
        ).pack(side="left", padx=10, pady=5)
        
        # Auto-Pair options frame
        auto_pair_options_frame = ctk.CTkFrame(pairs_header_frame)
        auto_pair_options_frame.pack(side="right", padx=10, pady=5)
        
        if not hasattr(self, 'stage_x_pairing_mode_var'):
            self.stage_x_pairing_mode_var = ctk.StringVar(value="common")
        
        ctk.CTkRadioButton(
            auto_pair_options_frame,
            text="Common PDF",
            variable=self.stage_x_pairing_mode_var,
            value="common",
            font=ctk.CTkFont(size=11)
        ).pack(side="left", padx=5)
        
        ctk.CTkRadioButton(
            auto_pair_options_frame,
            text="Pair by Book/Chapter",
            variable=self.stage_x_pairing_mode_var,
            value="pair",
            font=ctk.CTkFont(size=11)
        ).pack(side="left", padx=5)
        
        ctk.CTkButton(
            pairs_header_frame,
            text="Auto-Pair",
            command=self._auto_pair_stage_x_files,
            width=120,
            height=30,
            fg_color="green",
            hover_color="darkgreen"
        ).pack(side="right", padx=10, pady=5)
        
        # Pairs list (scrollable)
        pairs_list_frame = ctk.CTkFrame(pairs_frame)
        pairs_list_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            pairs_list_frame,
            text="File Pairs:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_x_pairs_list_scroll'):
            self.stage_x_pairs_list_scroll = ctk.CTkScrollableFrame(pairs_list_frame, height=150)
        self.stage_x_pairs_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize pairs
        if not hasattr(self, 'stage_x_pairs'):
            self.stage_x_pairs = []
        if not hasattr(self, 'stage_x_pairs_info_list'):
            self.stage_x_pairs_info_list = []
        
        # Part 2: Change Detection Settings
        part2_frame = ctk.CTkFrame(main_frame)
        part2_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(part2_frame, text="Part 2: Change Detection Settings",
                    font=ctk.CTkFont(size=16, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        # Change Detection Prompt Section
        change_prompt_frame = ctk.CTkFrame(part2_frame)
        change_prompt_frame.pack(fill="x", padx=10, pady=5)
        ctk.CTkLabel(change_prompt_frame, text="Change Detection Prompt:",
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=5, pady=5)
        
        # Change Detection Prompt mode (default vs custom)
        change_prompt_mode_frame = ctk.CTkFrame(change_prompt_frame)
        change_prompt_mode_frame.pack(fill="x", padx=5, pady=(0, 5))
        if not hasattr(self, 'stage_x_change_prompt_type_var'):
            self.stage_x_change_prompt_type_var = ctk.StringVar(value="default")
        ctk.CTkRadioButton(
            change_prompt_mode_frame,
            text="Use Default Prompt",
            variable=self.stage_x_change_prompt_type_var,
            value="default",
            command=self.on_stage_x_change_prompt_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)
        ctk.CTkRadioButton(
            change_prompt_mode_frame,
            text="Use Custom Prompt",
            variable=self.stage_x_change_prompt_type_var,
            value="custom",
            command=self.on_stage_x_change_prompt_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)
        
        # Default Change Detection Prompt combobox
        change_prompt_default_frame = ctk.CTkFrame(change_prompt_frame)
        change_prompt_default_frame.pack(fill="x", padx=5, pady=(0, 5))
        ctk.CTkLabel(
            change_prompt_default_frame,
            text="Default Change Detection Prompt:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", pady=(0, 5))
        
        change_prompt_names = self.prompt_manager.get_prompt_names()
        preferred_change_prompt_name = "Change Detection Prompt"
        if preferred_change_prompt_name in change_prompt_names:
            change_prompt_default_value = preferred_change_prompt_name
        else:
            change_prompt_default_value = change_prompt_names[0] if change_prompt_names else ""
        if not hasattr(self, 'stage_x_change_default_prompt_var'):
            self.stage_x_change_default_prompt_var = ctk.StringVar(value=change_prompt_default_value)
        if not hasattr(self, 'stage_x_change_default_prompt_combo'):
            self.stage_x_change_default_prompt_combo = ctk.CTkComboBox(
                change_prompt_default_frame,
                values=change_prompt_names,
                variable=self.stage_x_change_default_prompt_var,
                width=400,
                command=self.on_stage_x_change_default_prompt_selected,
            )
        self.stage_x_change_default_prompt_combo.pack(anchor="w", pady=(0, 5))
        
        # Textbox for Change Detection prompt
        if not hasattr(self, 'stage_x_change_prompt_text'):
            self.stage_x_change_prompt_text = ctk.CTkTextbox(change_prompt_frame, height=150, font=self.farsi_text_font)
        self.stage_x_change_prompt_text.pack(fill="x", padx=5, pady=(0, 5))
        
        # Initialize Change Detection prompt textbox
        self.on_stage_x_change_prompt_type_change()
        
        # Change Detection Model
        change_model_frame = ctk.CTkFrame(part2_frame)
        change_model_frame.pack(fill="x", padx=10, pady=5)
        ctk.CTkLabel(change_model_frame, text="Change Detection Model:", width=150).pack(side="left", padx=5)
        if not hasattr(self, 'stage_x_change_model_var'):
            default_model = self.model_var.get() if hasattr(self, 'model_var') else "gemini-2.5-pro"
            self.stage_x_change_model_var = ctk.StringVar(value=default_model)
        ctk.CTkComboBox(change_model_frame, values=APIConfig.TEXT_MODELS, variable=self.stage_x_change_model_var, width=300).pack(side="left", padx=5)
        
        # Delay Setting
        delay_frame = ctk.CTkFrame(main_frame)
        delay_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(delay_frame, text="Delay Between Pairs (seconds):", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_x_delay_var'):
            self.stage_x_delay_var = ctk.StringVar(value="5")
        
        delay_entry_frame = ctk.CTkFrame(delay_frame)
        delay_entry_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(delay_entry_frame, text="Delay:").pack(side="left", padx=5)
        delay_entry = ctk.CTkEntry(delay_entry_frame, textvariable=self.stage_x_delay_var, width=100)
        delay_entry.pack(side="left", padx=5)
        ctk.CTkLabel(delay_entry_frame, text="seconds", text_color="gray").pack(side="left", padx=5)
        
        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)
        
        if not hasattr(self, 'stage_x_process_btn'):
            self.stage_x_process_btn = ctk.CTkButton(
                process_btn_frame,
                text="Process All Pairs",
                command=self.process_stage_x_batch,
                font=ctk.CTkFont(size=16, weight="bold"),
                height=40
            )
        self.stage_x_process_btn.pack(pady=10)
        
        # Progress bar for batch processing
        if not hasattr(self, 'stage_x_progress_bar'):
            self.stage_x_progress_bar = ctk.CTkProgressBar(process_btn_frame, width=400)
        self.stage_x_progress_bar.pack(pady=10)
        self.stage_x_progress_bar.set(0)
        
        # Status Label
        if not hasattr(self, 'stage_x_status_label'):
            self.stage_x_status_label = ctk.CTkLabel(
                process_btn_frame,
                text="Ready",
                font=ctk.CTkFont(size=12),
                text_color="gray"
            )
        self.stage_x_status_label.pack(pady=5)
    
    def setup_stage_y_ui(self, parent):
        """Setup UI for Stage Y: Deletion Detection"""
        main_frame = ctk.CTkScrollableFrame(parent)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Navigation button
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
        
        title = ctk.CTkLabel(main_frame, text="Deletion Detection", 
                    font=ctk.CTkFont(size=24, weight="bold"))
        title.pack(pady=(0, 20))
        
        desc = ctk.CTkLabel(
            main_frame,
            text="Step 1: Extract PDF from old reference (2 parts). Step 2: Detect deletions by comparing OCR Extraction JSON with extracted PDF - Batch Processing.",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        desc.pack(pady=(0, 20))
        
        # Old Reference PDF Files Selection - Batch Processing
        old_pdf_frame = ctk.CTkFrame(main_frame)
        old_pdf_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(
            old_pdf_frame,
            text="Old Reference PDF Files - Batch Processing",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=5)
        
        # File selection buttons for PDF
        buttons_frame_pdf = ctk.CTkFrame(old_pdf_frame)
        buttons_frame_pdf.pack(fill="x", padx=10, pady=5)
        
        def browse_multiple_pdf_files():
            filenames = filedialog.askopenfilenames(
                title="Select Old Reference PDF files",
                filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")]
            )
            if filenames:
                if not hasattr(self, 'stage_y_selected_pdf_files'):
                    self.stage_y_selected_pdf_files = []
                for filename in filenames:
                    if filename not in self.stage_y_selected_pdf_files:
                        self.stage_y_selected_pdf_files.append(filename)
                        self._add_stage_y_pdf_file_to_ui(filename)
                self._update_stage_y_pairs()
        
        def select_folder_pdf_files():
            folder_path = filedialog.askdirectory(
                title="Select folder containing PDF files"
            )
            if not folder_path:
                return
            
            import glob
            pdf_files = glob.glob(os.path.join(folder_path, "*.pdf"))
            pdf_files = [f for f in pdf_files if os.path.isfile(f)]
            
            if not pdf_files:
                messagebox.showinfo("Info", "No PDF files found in selected folder")
                return
            
            if not hasattr(self, 'stage_y_selected_pdf_files'):
                self.stage_y_selected_pdf_files = []
            
            added_count = 0
            for pdf_file in pdf_files:
                if pdf_file not in self.stage_y_selected_pdf_files:
                    self.stage_y_selected_pdf_files.append(pdf_file)
                    self._add_stage_y_pdf_file_to_ui(pdf_file)
                    added_count += 1
            
            if added_count > 0:
                self._update_stage_y_pairs()
            messagebox.showinfo("Success", f"Added {added_count} PDF file(s) from folder")
        
        ctk.CTkButton(
            buttons_frame_pdf,
            text="Browse Multiple PDF Files",
            command=browse_multiple_pdf_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        ctk.CTkButton(
            buttons_frame_pdf,
            text="Select Folder (PDF)",
            command=select_folder_pdf_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        # PDF files list (scrollable)
        list_frame_pdf = ctk.CTkFrame(old_pdf_frame)
        list_frame_pdf.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            list_frame_pdf,
            text="Selected PDF Files:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_y_pdf_files_list_scroll'):
            self.stage_y_pdf_files_list_scroll = ctk.CTkScrollableFrame(list_frame_pdf, height=100)
        self.stage_y_pdf_files_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize selected files list
        if not hasattr(self, 'stage_y_selected_pdf_files'):
            self.stage_y_selected_pdf_files = []
        
        # Step 1: OCR Extraction Settings
        step1_settings_frame = ctk.CTkFrame(main_frame)
        step1_settings_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(step1_settings_frame, text="Step 1: OCR Extraction Settings",
                    font=ctk.CTkFont(size=16, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        # Step 1: OCR Extraction Prompt
        ocr_prompt_frame = ctk.CTkFrame(main_frame)
        ocr_prompt_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(ocr_prompt_frame, text="OCR Extraction Prompt (Step 1):",
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_y_ocr_prompt_text'):
            self.stage_y_ocr_prompt_text = ctk.CTkTextbox(ocr_prompt_frame, height=100, font=self.farsi_text_font)
        self.stage_y_ocr_prompt_text.pack(fill="x", padx=10, pady=(0, 10))
        
        # Step 1: OCR Extraction Model
        ocr_model_frame = ctk.CTkFrame(main_frame)
        ocr_model_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(ocr_model_frame, text="OCR Extraction Model (Step 1):",
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_y_ocr_model_var'):
            self.stage_y_ocr_model_var = ctk.StringVar(value="gemini-2.5-pro")
        ctk.CTkComboBox(ocr_model_frame, values=APIConfig.TEXT_MODELS, variable=self.stage_y_ocr_model_var, width=400).pack(anchor="w", padx=10, pady=(0, 10))
        
        # OCR Extraction JSON Files Selection - Batch Processing
        ocr_extraction_frame = ctk.CTkFrame(main_frame)
        ocr_extraction_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(
            ocr_extraction_frame,
            text="OCR Extraction JSON Files (Step 2 - from OCR Extraction stage) - Batch Processing",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=5)
        
        # File selection buttons for OCR Extraction
        buttons_frame_ocr = ctk.CTkFrame(ocr_extraction_frame)
        buttons_frame_ocr.pack(fill="x", padx=10, pady=5)
        
        def browse_multiple_ocr_extraction_files():
            filenames = filedialog.askopenfilenames(
                title="Select OCR Extraction JSON files",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            if filenames:
                if not hasattr(self, 'stage_y_selected_ocr_extraction_files'):
                    self.stage_y_selected_ocr_extraction_files = []
                for filename in filenames:
                    if filename not in self.stage_y_selected_ocr_extraction_files:
                        self.stage_y_selected_ocr_extraction_files.append(filename)
                        self._add_stage_y_ocr_extraction_file_to_ui(filename)
                self._update_stage_y_pairs()
        
        def select_folder_ocr_extraction_files():
            folder_path = filedialog.askdirectory(
                title="Select folder containing OCR Extraction JSON files"
            )
            if not folder_path:
                return
            
            import glob
            json_files = glob.glob(os.path.join(folder_path, "*.json"))
            json_files = [f for f in json_files if os.path.isfile(f)]
            
            if not json_files:
                messagebox.showinfo("Info", "No JSON files found in selected folder")
                return
            
            if not hasattr(self, 'stage_y_selected_ocr_extraction_files'):
                self.stage_y_selected_ocr_extraction_files = []
            
            added_count = 0
            for json_file in json_files:
                if json_file not in self.stage_y_selected_ocr_extraction_files:
                    self.stage_y_selected_ocr_extraction_files.append(json_file)
                    self._add_stage_y_ocr_extraction_file_to_ui(json_file)
                    added_count += 1
            
            if added_count > 0:
                self._update_stage_y_pairs()
            messagebox.showinfo("Success", f"Added {added_count} JSON file(s) from folder")
        
        ctk.CTkButton(
            buttons_frame_ocr,
            text="Browse Multiple OCR Extraction Files",
            command=browse_multiple_ocr_extraction_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        ctk.CTkButton(
            buttons_frame_ocr,
            text="Select Folder (OCR Extraction)",
            command=select_folder_ocr_extraction_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        # OCR Extraction files list (scrollable)
        list_frame_ocr = ctk.CTkFrame(ocr_extraction_frame)
        list_frame_ocr.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            list_frame_ocr,
            text="Selected OCR Extraction Files:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_y_ocr_extraction_files_list_scroll'):
            self.stage_y_ocr_extraction_files_list_scroll = ctk.CTkScrollableFrame(list_frame_ocr, height=100)
        self.stage_y_ocr_extraction_files_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize selected files list
        if not hasattr(self, 'stage_y_selected_ocr_extraction_files'):
            self.stage_y_selected_ocr_extraction_files = []
        
        # Pairs Section
        pairs_frame = ctk.CTkFrame(main_frame)
        pairs_frame.pack(fill="x", pady=10)
        
        pairs_header_frame = ctk.CTkFrame(pairs_frame)
        pairs_header_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(
            pairs_header_frame,
            text="Pairs (PDF ↔ OCR Extraction):",
            font=ctk.CTkFont(size=14, weight="bold"),
        ).pack(side="left", padx=10, pady=5)
        
        # Auto-Pair options frame
        auto_pair_options_frame = ctk.CTkFrame(pairs_header_frame)
        auto_pair_options_frame.pack(side="right", padx=10, pady=5)
        
        if not hasattr(self, 'stage_y_pairing_mode_var'):
            self.stage_y_pairing_mode_var = ctk.StringVar(value="common")
        
        ctk.CTkRadioButton(
            auto_pair_options_frame,
            text="Common PDF",
            variable=self.stage_y_pairing_mode_var,
            value="common",
            font=ctk.CTkFont(size=11)
        ).pack(side="left", padx=5)
        
        ctk.CTkRadioButton(
            auto_pair_options_frame,
            text="Pair by Book/Chapter",
            variable=self.stage_y_pairing_mode_var,
            value="pair",
            font=ctk.CTkFont(size=11)
        ).pack(side="left", padx=5)
        
        ctk.CTkButton(
            pairs_header_frame,
            text="Auto-Pair",
            command=self._auto_pair_stage_y_files,
            width=120,
            height=30,
            fg_color="green",
            hover_color="darkgreen"
        ).pack(side="right", padx=10, pady=5)
        
        # Pairs list (scrollable)
        pairs_list_frame = ctk.CTkFrame(pairs_frame)
        pairs_list_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            pairs_list_frame,
            text="File Pairs:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_y_pairs_list_scroll'):
            self.stage_y_pairs_list_scroll = ctk.CTkScrollableFrame(pairs_list_frame, height=150)
        self.stage_y_pairs_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize pairs
        if not hasattr(self, 'stage_y_pairs'):
            self.stage_y_pairs = []
        if not hasattr(self, 'stage_y_pairs_info_list'):
            self.stage_y_pairs_info_list = []
        
        # Step 2: Deletion Detection Settings
        step2_settings_frame = ctk.CTkFrame(main_frame)
        step2_settings_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(step2_settings_frame, text="Step 2: Deletion Detection Settings",
                    font=ctk.CTkFont(size=16, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        # Step 2: Deletion Detection Prompt
        deletion_prompt_frame = ctk.CTkFrame(main_frame)
        deletion_prompt_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(deletion_prompt_frame, text="Deletion Detection Prompt (Step 2):",
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        # Stage Y deletion prompt mode (default vs custom)
        stage_y_deletion_mode_frame = ctk.CTkFrame(deletion_prompt_frame)
        stage_y_deletion_mode_frame.pack(fill="x", padx=10, pady=(0, 5))
        if not hasattr(self, 'stage_y_deletion_prompt_type_var'):
            self.stage_y_deletion_prompt_type_var = ctk.StringVar(value="default")
        ctk.CTkRadioButton(
            stage_y_deletion_mode_frame,
            text="Use Default Prompt",
            variable=self.stage_y_deletion_prompt_type_var,
            value="default",
            command=self.on_stage_y_deletion_prompt_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)
        ctk.CTkRadioButton(
            stage_y_deletion_mode_frame,
            text="Use Custom Prompt",
            variable=self.stage_y_deletion_prompt_type_var,
            value="custom",
            command=self.on_stage_y_deletion_prompt_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)
        
        # Default Stage Y deletion prompt combobox
        stage_y_deletion_default_frame = ctk.CTkFrame(deletion_prompt_frame)
        stage_y_deletion_default_frame.pack(fill="x", padx=10, pady=(0, 10))
        
        ctk.CTkLabel(
            stage_y_deletion_default_frame,
            text="Default Deletion Detection Prompt:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", pady=(0, 5))
        
        stage_y_prompt_names = self.prompt_manager.get_prompt_names()
        preferred_stage_y_name = "Deletion Detection Prompt"
        if preferred_stage_y_name in stage_y_prompt_names:
            stage_y_deletion_default_value = preferred_stage_y_name
        else:
            stage_y_deletion_default_value = stage_y_prompt_names[0] if stage_y_prompt_names else ""
        if not hasattr(self, 'stage_y_deletion_default_prompt_var'):
            self.stage_y_deletion_default_prompt_var = ctk.StringVar(value=stage_y_deletion_default_value)
        if not hasattr(self, 'stage_y_deletion_default_prompt_combo'):
            self.stage_y_deletion_default_prompt_combo = ctk.CTkComboBox(
                stage_y_deletion_default_frame,
                values=stage_y_prompt_names,
                variable=self.stage_y_deletion_default_prompt_var,
                width=400,
                command=self.on_stage_y_deletion_default_prompt_selected,
            )
        self.stage_y_deletion_default_prompt_combo.pack(anchor="w", pady=(0, 5))
        
        # Textbox for Stage Y deletion prompt
        if not hasattr(self, 'stage_y_deletion_prompt_text'):
            self.stage_y_deletion_prompt_text = ctk.CTkTextbox(deletion_prompt_frame, height=150, font=self.farsi_text_font)
        self.stage_y_deletion_prompt_text.pack(fill="x", padx=10, pady=(0, 10))
        
        # Initialize Stage Y deletion prompt textbox
        self.on_stage_y_deletion_prompt_type_change()
        if hasattr(self, 'stage_y_deletion_default_prompt_var'):
            default_prompt_name = self.stage_y_deletion_default_prompt_var.get()
            if default_prompt_name:
                self.on_stage_y_deletion_default_prompt_selected(default_prompt_name)
        
        # Step 2: Deletion Detection Model
        deletion_model_frame = ctk.CTkFrame(step2_settings_frame)
        deletion_model_frame.pack(fill="x", padx=10, pady=5)
        ctk.CTkLabel(deletion_model_frame, text="Deletion Detection Model (Step 2):", width=200).pack(side="left", padx=5)
        if not hasattr(self, 'stage_y_deletion_model_var'):
            default_model = self.model_var.get() if hasattr(self, 'model_var') else "gemini-2.5-pro"
            self.stage_y_deletion_model_var = ctk.StringVar(value=default_model)
        ctk.CTkComboBox(deletion_model_frame, values=APIConfig.TEXT_MODELS, variable=self.stage_y_deletion_model_var, width=300).pack(side="left", padx=5)
        
        # Delay Setting
        delay_frame = ctk.CTkFrame(main_frame)
        delay_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(delay_frame, text="Delay Between Pairs (seconds):", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_y_delay_var'):
            self.stage_y_delay_var = ctk.StringVar(value="5")
        
        delay_entry_frame = ctk.CTkFrame(delay_frame)
        delay_entry_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(delay_entry_frame, text="Delay:").pack(side="left", padx=5)
        delay_entry = ctk.CTkEntry(delay_entry_frame, textvariable=self.stage_y_delay_var, width=100)
        delay_entry.pack(side="left", padx=5)
        ctk.CTkLabel(delay_entry_frame, text="seconds", text_color="gray").pack(side="left", padx=5)
        
        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)
        
        if not hasattr(self, 'stage_y_process_btn'):
            self.stage_y_process_btn = ctk.CTkButton(
                process_btn_frame,
                text="Process All Pairs",
                command=self.process_stage_y_batch,
                font=ctk.CTkFont(size=16, weight="bold"),
                height=40
            )
        self.stage_y_process_btn.pack(pady=10)
        
        # Progress bar for batch processing
        if not hasattr(self, 'stage_y_progress_bar'):
            self.stage_y_progress_bar = ctk.CTkProgressBar(process_btn_frame, width=400)
        self.stage_y_progress_bar.pack(pady=10)
        self.stage_y_progress_bar.set(0)
        
        # Status Label
        if not hasattr(self, 'stage_y_status_label'):
            self.stage_y_status_label = ctk.CTkLabel(
                process_btn_frame,
                text="Ready",
                font=ctk.CTkFont(size=12),
                text_color="gray"
            )
        self.stage_y_status_label.pack(pady=5)
    
    def _add_stage_y_pdf_file_to_ui(self, file_path: str):
        """Add a PDF file to the UI list"""
        file_frame = ctk.CTkFrame(self.stage_y_pdf_files_list_scroll)
        file_frame.pack(fill="x", padx=5, pady=2)
        
        file_name = os.path.basename(file_path)
        
        name_label = ctk.CTkLabel(
            file_frame,
            text=file_name,
            font=ctk.CTkFont(size=11),
            anchor="w"
        )
        name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
        
        def remove_file():
            if file_path in self.stage_y_selected_pdf_files:
                self.stage_y_selected_pdf_files.remove(file_path)
            file_frame.destroy()
            self._update_stage_y_pairs()
        
        remove_btn = ctk.CTkButton(
            file_frame,
            text="✕",
            command=remove_file,
            width=30,
            height=25,
            fg_color="red",
            hover_color="darkred"
        )
        remove_btn.pack(side="right", padx=5, pady=5)
    
    def _add_stage_y_ocr_extraction_file_to_ui(self, file_path: str):
        """Add an OCR Extraction file to the UI list"""
        file_frame = ctk.CTkFrame(self.stage_y_ocr_extraction_files_list_scroll)
        file_frame.pack(fill="x", padx=5, pady=2)
        
        file_name = os.path.basename(file_path)
        
        name_label = ctk.CTkLabel(
            file_frame,
            text=file_name,
            font=ctk.CTkFont(size=11),
            anchor="w"
        )
        name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
        
        def remove_file():
            if file_path in self.stage_y_selected_ocr_extraction_files:
                self.stage_y_selected_ocr_extraction_files.remove(file_path)
            file_frame.destroy()
            self._update_stage_y_pairs()
        
        remove_btn = ctk.CTkButton(
            file_frame,
            text="✕",
            command=remove_file,
            width=30,
            height=25,
            fg_color="red",
            hover_color="darkred"
        )
        remove_btn.pack(side="right", padx=5, pady=5)
    
    def _extract_book_chapter_from_ocr_extraction_for_y(self, ocr_extraction_path: str):
        """Extract book and chapter from OCR Extraction JSON file (from metadata or filename)"""
        try:
            # Try to load OCR Extraction JSON and extract from metadata
            data = json.load(open(ocr_extraction_path, 'r', encoding='utf-8'))
            metadata = self.get_metadata_from_json(data)
            book_id = metadata.get("book_id")
            chapter_id = metadata.get("chapter_id")
            if book_id is not None and chapter_id is not None:
                return book_id, chapter_id
        except:
            pass
        
        # Fallback: try to extract from filename
        try:
            basename = os.path.basename(ocr_extraction_path)
            name_without_ext = os.path.splitext(basename)[0]
            import re
            # Try various patterns: {prefix}{book}{chapter}+{name} or {prefix}{book}{chapter}
            match = re.match(r'^[a-z]?(\d{3})(\d{3})\+', name_without_ext)
            if match:
                book_id = int(match.group(1))
                chapter_id = int(match.group(2))
                return book_id, chapter_id
            # Try pattern without +: {prefix}{book}{chapter}
            if len(name_without_ext) >= 7:
                # Try to extract 6 digits
                digits_match = re.search(r'(\d{3})(\d{3})', name_without_ext)
                if digits_match:
                    book_id = int(digits_match.group(1))
                    chapter_id = int(digits_match.group(2))
                    return book_id, chapter_id
        except:
            pass
        
        return None, None
    
    def _auto_pair_stage_y_files(self):
        """Auto-pair PDF files with OCR Extraction files"""
        pairing_mode = self.stage_y_pairing_mode_var.get() if hasattr(self, 'stage_y_pairing_mode_var') else "common"
        
        if not hasattr(self, 'stage_y_selected_pdf_files') or not self.stage_y_selected_pdf_files:
            messagebox.showwarning("Warning", "Please add at least one PDF file")
            return
        
        if not hasattr(self, 'stage_y_selected_ocr_extraction_files') or not self.stage_y_selected_ocr_extraction_files:
            messagebox.showwarning("Warning", "Please add at least one OCR Extraction file")
            return
        
        pairs = []
        
        if pairing_mode == "common":
            # Use first PDF for all OCR Extraction files
            common_pdf = self.stage_y_selected_pdf_files[0]
            for ocr_extraction_path in self.stage_y_selected_ocr_extraction_files:
                book_id, chapter_id = self._extract_book_chapter_from_ocr_extraction_for_y(ocr_extraction_path)
                pair = {
                    'pdf_path': common_pdf,
                    'ocr_extraction_path': ocr_extraction_path,
                    'book_id': book_id,
                    'chapter_id': chapter_id,
                    'status': 'pending',
                    'output_path': None,
                    'error': None
                }
                pairs.append(pair)
        else:
            # Pair by Book/Chapter (if multiple PDFs, try to match)
            # For now, use first PDF for all (can be enhanced later)
            if len(self.stage_y_selected_pdf_files) == 1:
                common_pdf = self.stage_y_selected_pdf_files[0]
                for ocr_extraction_path in self.stage_y_selected_ocr_extraction_files:
                    book_id, chapter_id = self._extract_book_chapter_from_ocr_extraction_for_y(ocr_extraction_path)
                    pair = {
                        'pdf_path': common_pdf,
                        'ocr_extraction_path': ocr_extraction_path,
                        'book_id': book_id,
                        'chapter_id': chapter_id,
                        'status': 'pending',
                        'output_path': None,
                        'error': None
                    }
                    pairs.append(pair)
            else:
                # Multiple PDFs: try to pair by Book/Chapter
                paired_pdfs = set()
                for ocr_extraction_path in self.stage_y_selected_ocr_extraction_files:
                    book_id, chapter_id = self._extract_book_chapter_from_ocr_extraction_for_y(ocr_extraction_path)
                    
                    if book_id is None or chapter_id is None:
                        # Can't extract book/chapter, use first PDF
                        matched_pdf = self.stage_y_selected_pdf_files[0]
                    else:
                        # Try to find matching PDF (for now, use first available)
                        matched_pdf = None
                        for pdf_path in self.stage_y_selected_pdf_files:
                            if pdf_path not in paired_pdfs:
                                matched_pdf = pdf_path
                                paired_pdfs.add(pdf_path)
                                break
                        
                        if not matched_pdf:
                            matched_pdf = self.stage_y_selected_pdf_files[0]
                    
                    pair = {
                        'pdf_path': matched_pdf,
                        'ocr_extraction_path': ocr_extraction_path,
                        'book_id': book_id,
                        'chapter_id': chapter_id,
                        'status': 'pending',
                        'output_path': None,
                        'error': None
                    }
                    pairs.append(pair)
        
        self.stage_y_pairs = pairs
        self._update_stage_y_pairs_ui()
        
        paired_count = len(pairs)
        messagebox.showinfo(
            "Auto-Pairing Complete",
            f"Created {paired_count} pair(s)"
        )
    
    def _update_stage_y_pairs(self):
        """Update pairs when files are added/removed"""
        # Clear existing pairs UI
        for widget in self.stage_y_pairs_list_scroll.winfo_children():
            widget.destroy()
        
        self.stage_y_pairs_info_list = []
        
        # Re-pair if we have pairs
        if hasattr(self, 'stage_y_pairs') and self.stage_y_pairs:
            self._update_stage_y_pairs_ui()
    
    def _update_stage_y_pairs_ui(self):
        """Update the pairs UI display"""
        # Clear existing pairs UI
        for widget in self.stage_y_pairs_list_scroll.winfo_children():
            widget.destroy()
        
        self.stage_y_pairs_info_list = []
        
        if not hasattr(self, 'stage_y_pairs') or not self.stage_y_pairs:
            return
        
        for pair in self.stage_y_pairs:
            pair_frame = ctk.CTkFrame(self.stage_y_pairs_list_scroll)
            pair_frame.pack(fill="x", padx=5, pady=2)
            
            pdf_name = os.path.basename(pair['pdf_path']) if pair['pdf_path'] else "None"
            ocr_name = os.path.basename(pair['ocr_extraction_path']) if pair['ocr_extraction_path'] else "None"
            
            pair_text = f"{pdf_name} ↔ {ocr_name}"
            
            # Main pair info
            info_frame = ctk.CTkFrame(pair_frame)
            info_frame.pack(fill="x", padx=5, pady=2)
            
            name_label = ctk.CTkLabel(
                info_frame,
                text=pair_text,
                font=ctk.CTkFont(size=11),
                anchor="w"
            )
            name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
            
            status_label = ctk.CTkLabel(
                info_frame,
                text=pair.get('status', 'pending').upper(),
                font=ctk.CTkFont(size=10),
                text_color="gray" if pair.get('status') == 'pending' else 
                          "green" if pair.get('status') == 'completed' else "red"
            )
            status_label.pack(side="right", padx=10, pady=5)
            
            self.stage_y_pairs_info_list.append({
                'pair': pair,
                'status_label': status_label,
                'frame': pair_frame
            })
    
    def process_stage_y_batch(self):
        """Process multiple PDF + OCR Extraction pairs for Stage Y"""
        def worker():
            try:
                if not hasattr(self, 'stage_y_pairs') or not self.stage_y_pairs:
                    self.root.after(0, lambda: messagebox.showwarning(
                        "Warning",
                        "Please add files and create pairs first. Click 'Auto-Pair' button."
                    ))
                    return
                
                # Filter pairs that have both PDF and OCR Extraction file
                valid_pairs = [p for p in self.stage_y_pairs if p['pdf_path'] and p['ocr_extraction_path']]
                
                if not valid_pairs:
                    self.root.after(0, lambda: messagebox.showwarning(
                        "Warning",
                        "No valid pairs found. Each pair must have both PDF and OCR Extraction file."
                    ))
                    return
                
                self.stage_y_process_btn.configure(
                    state="disabled",
                    text="Processing Batch..."
                )
                
                # Get prompts
                ocr_prompt = self.stage_y_ocr_prompt_text.get("1.0", tk.END).strip()
                if not ocr_prompt:
                    default_ocr_prompt = self.prompt_manager.get_prompt("OCR Extraction Prompt")
                    if default_ocr_prompt:
                        ocr_prompt = default_ocr_prompt
                        self.logger.info("Using default OCR extraction prompt from prompts.json")
                    else:
                        self.root.after(0, lambda: messagebox.showerror("Error", "Please enter an OCR extraction prompt"))
                        return
                
                deletion_prompt = self.stage_y_deletion_prompt_text.get("1.0", tk.END).strip()
                if not deletion_prompt:
                    default_deletion_prompt = self.prompt_manager.get_prompt("Deletion Detection Prompt")
                    if default_deletion_prompt:
                        deletion_prompt = default_deletion_prompt
                        self.logger.info("Using default deletion detection prompt from prompts.json")
                    else:
                        self.root.after(0, lambda: messagebox.showerror("Error", "Please enter a deletion detection prompt"))
                        return
                
                # Get models
                ocr_model = self.stage_y_ocr_model_var.get() if hasattr(self, 'stage_y_ocr_model_var') else "gemini-2.5-pro"
                deletion_model = self.stage_y_deletion_model_var.get() if hasattr(self, 'stage_y_deletion_model_var') else "gemini-2.5-pro"
                
                # Get delay
                try:
                    delay_seconds = float(self.stage_y_delay_var.get() if hasattr(self, 'stage_y_delay_var') else "5")
                    if delay_seconds < 0:
                        delay_seconds = 0
                except:
                    delay_seconds = 5
                
                total_pairs = len(valid_pairs)
                completed = 0
                failed = 0
                
                # Step 1 output cache: {pdf_path: step1_output_path}
                step1_cache = {}
                
                # Reset progress bar
                self.root.after(0, lambda: self.stage_y_progress_bar.set(0))
                
                # Process each pair
                for idx, pair in enumerate(valid_pairs):
                    pdf_path = pair['pdf_path']
                    ocr_extraction_path = pair['ocr_extraction_path']
                    
                    pdf_name = os.path.basename(pdf_path)
                    ocr_name = os.path.basename(ocr_extraction_path)
                    
                    # Update status to processing
                    if hasattr(self, 'stage_y_pairs_info_list'):
                        for pair_info in self.stage_y_pairs_info_list:
                            if pair_info['pair'] == pair:
                                self.root.after(0, lambda sl=pair_info['status_label']:
                                               sl.configure(text="PROCESSING", text_color="blue"))
                                break
                    
                    # Update progress label
                    self.root.after(0, lambda idx=idx, total=total_pairs, pdf=pdf_name, ocr=ocr_name:
                                   self.stage_y_status_label.configure(
                                       text=f"Processing pair {idx+1}/{total}: {pdf} ↔ {ocr}",
                                       text_color="blue"
                                   ))
                    
                    # Progress bar
                    progress = idx / total_pairs
                    self.root.after(0, lambda p=progress: self.stage_y_progress_bar.set(p))
                    
                    try:
                        def progress_callback(msg: str):
                            self.root.after(0, lambda m=msg:
                                           self.stage_y_status_label.configure(text=m))
                        
                        # Check cache for Step 1 output
                        if pdf_path not in step1_cache:
                            # Step 1: Extract PDF (one time per PDF)
                            self.root.after(0, lambda: self.stage_y_status_label.configure(
                                text=f"Step 1: Extracting PDF: {pdf_name}..."))
                            
                            # Determine output directory for Step 1
                            step1_output_dir = self.get_default_output_dir(ocr_extraction_path)
                            
                            # Extract book and chapter from OCR Extraction JSON for Step 1
                            book_id, chapter_id = self._extract_book_chapter_from_ocr_extraction_for_y(ocr_extraction_path)
                            if book_id is None:
                                book_id = 105  # default
                            if chapter_id is None:
                                chapter_id = 3  # default
                            
                            # Call Step 1 extraction
                            step1_output = self.stage_y_processor._step1_extract_pdf(
                                pdf_path=pdf_path,
                                prompt=ocr_prompt,
                                model_name=ocr_model,
                                output_dir=step1_output_dir,
                                book_id=book_id,
                                chapter_id=chapter_id,
                                progress_callback=progress_callback
                            )
                            
                            if not step1_output or not os.path.exists(step1_output):
                                raise Exception(f"Step 1 (PDF extraction) failed for {pdf_name}")
                            
                            step1_cache[pdf_path] = step1_output
                            self.logger.info(f"Cached Step 1 output: {pdf_path} -> {step1_output}")
                        else:
                            step1_output = step1_cache[pdf_path]
                            self.logger.info(f"Using cached Step 1 output: {pdf_path}")
                        
                        # Step 2: Detect deletions
                        self.root.after(0, lambda: self.stage_y_status_label.configure(
                            text=f"Step 2: Detecting deletions: {ocr_name}..."))
                        
                        # Process Stage Y (use cached Step 1 output)
                        output_path = self.stage_y_processor.process_stage_y(
                            old_book_pdf_path=pdf_path,
                            ocr_extraction_prompt=ocr_prompt,
                            ocr_extraction_model=ocr_model,
                            ocr_extraction_json_path=ocr_extraction_path,
                            deletion_detection_prompt=deletion_prompt,
                            deletion_detection_model=deletion_model,
                            output_dir=self.get_default_output_dir(ocr_extraction_path),
                            progress_callback=progress_callback,
                            step1_output_path=step1_output  # Use cached Step 1 output
                        )
                        
                        if output_path and os.path.exists(output_path):
                            completed += 1
                            pair['output_path'] = output_path
                            pair['status'] = 'completed'
                            
                            # Update status to completed
                            if hasattr(self, 'stage_y_pairs_info_list'):
                                for pair_info in self.stage_y_pairs_info_list:
                                    if pair_info['pair'] == pair:
                                        self.root.after(0, lambda sl=pair_info['status_label']:
                                                       sl.configure(text="COMPLETED", text_color="green"))
                                        break
                            
                            self.logger.info(
                                f"Successfully processed: {pdf_name} ↔ {ocr_name} -> {os.path.basename(output_path)}"
                            )
                        else:
                            failed += 1
                            pair['status'] = 'failed'
                            pair['error'] = "Processing returned no output"
                            
                            # Update status to failed
                            if hasattr(self, 'stage_y_pairs_info_list'):
                                for pair_info in self.stage_y_pairs_info_list:
                                    if pair_info['pair'] == pair:
                                        self.root.after(0, lambda sl=pair_info['status_label']:
                                                       sl.configure(text="FAILED", text_color="red"))
                                        break
                            
                            self.logger.error(f"Failed to process: {pdf_name} ↔ {ocr_name}")
                            
                    except Exception as e:
                        failed += 1
                        pair['status'] = 'failed'
                        pair['error'] = str(e)
                        self.logger.error(f"Error processing {pdf_name} ↔ {ocr_name}: {str(e)}", exc_info=True)
                        
                        # Update status to failed
                        if hasattr(self, 'stage_y_pairs_info_list'):
                            for pair_info in self.stage_y_pairs_info_list:
                                if pair_info['pair'] == pair:
                                    self.root.after(0, lambda sl=pair_info['status_label']:
                                                   sl.configure(text="FAILED", text_color="red"))
                                    break
                    
                    # Delay before next batch (except for the last one)
                    if idx < total_pairs - 1 and delay_seconds > 0:
                        self.root.after(0, lambda: self.stage_y_status_label.configure(
                            text=f"Waiting {delay_seconds} seconds before next batch..."))
                        import time
                        time.sleep(delay_seconds)
                
                # Final progress update
                self.root.after(0, lambda: self.stage_y_progress_bar.set(1.0))
                
                self.root.after(0, lambda: self.stage_y_status_label.configure(
                    text=f"Batch completed: {completed} succeeded, {failed} failed",
                    text_color="green" if failed == 0 else "orange"
                ))
                
                # Show summary
                self.root.after(0, lambda: messagebox.showinfo(
                    "Batch Processing Complete",
                    f"Stage Y batch completed!\n\n"
                    f"Total pairs: {total_pairs}\n"
                    f"Successful: {completed}\n"
                    f"Failed: {failed}"
                ))
            
            except Exception as e:
                self.logger.error(f"Error in batch Stage Y processing: {str(e)}", exc_info=True)
                self.root.after(0, lambda: messagebox.showerror(
                    "Error",
                    f"Batch processing error:\n{str(e)}"
                ))
            finally:
                self.root.after(0, lambda: self.stage_y_process_btn.configure(
                    state="normal",
                    text="Process All Pairs"
                ))
        
        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def setup_stage_z_ui(self, parent):
        """Setup UI for Stage Z: RichText Generation"""
        main_frame = ctk.CTkScrollableFrame(parent)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Navigation button
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
        
        title = ctk.CTkLabel(main_frame, text="RichText Generation", 
                    font=ctk.CTkFont(size=24, weight="bold"))
        title.pack(pady=(0, 20))
        
        desc = ctk.CTkLabel(
            main_frame,
            text="Generate RichText format output from Stage A, Stage X, and Stage Y data - Batch Processing.",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        desc.pack(pady=(0, 20))
        
        # Stage A Files Selection - Batch Processing
        stage_a_frame = ctk.CTkFrame(main_frame)
        stage_a_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(
            stage_a_frame,
            text="Stage A JSON Files (a files) - Batch Processing",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=5)
        
        # File selection buttons for Stage A
        buttons_frame_a = ctk.CTkFrame(stage_a_frame)
        buttons_frame_a.pack(fill="x", padx=10, pady=5)
        
        def browse_multiple_stage_a_files():
            filenames = filedialog.askopenfilenames(
                title="Select Stage A JSON files",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            if filenames:
                if not hasattr(self, 'stage_z_selected_stage_a_files'):
                    self.stage_z_selected_stage_a_files = []
                for filename in filenames:
                    if filename not in self.stage_z_selected_stage_a_files:
                        self.stage_z_selected_stage_a_files.append(filename)
                        self._add_stage_z_stage_a_file_to_ui(filename)
                self._update_stage_z_triples()
        
        def select_folder_stage_a_files():
            folder_path = filedialog.askdirectory(
                title="Select folder containing Stage A JSON files"
            )
            if not folder_path:
                return
            
            import glob
            json_files = glob.glob(os.path.join(folder_path, "*.json"))
            json_files = [f for f in json_files if os.path.isfile(f)]
            
            if not json_files:
                messagebox.showinfo("Info", "No JSON files found in selected folder")
                return
            
            if not hasattr(self, 'stage_z_selected_stage_a_files'):
                self.stage_z_selected_stage_a_files = []
            
            added_count = 0
            for json_file in json_files:
                if json_file not in self.stage_z_selected_stage_a_files:
                    self.stage_z_selected_stage_a_files.append(json_file)
                    self._add_stage_z_stage_a_file_to_ui(json_file)
                    added_count += 1
            
            if added_count > 0:
                self._update_stage_z_triples()
            messagebox.showinfo("Success", f"Added {added_count} JSON file(s) from folder")
        
        ctk.CTkButton(
            buttons_frame_a,
            text="Browse Multiple Stage A Files",
            command=browse_multiple_stage_a_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        ctk.CTkButton(
            buttons_frame_a,
            text="Select Folder (Stage A)",
            command=select_folder_stage_a_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        # Stage A files list (scrollable)
        list_frame_a = ctk.CTkFrame(stage_a_frame)
        list_frame_a.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            list_frame_a,
            text="Selected Stage A Files:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_z_stage_a_files_list_scroll'):
            self.stage_z_stage_a_files_list_scroll = ctk.CTkScrollableFrame(list_frame_a, height=100)
        self.stage_z_stage_a_files_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize selected files list
        if not hasattr(self, 'stage_z_selected_stage_a_files'):
            self.stage_z_selected_stage_a_files = []
        
        # Stage X Output Files Selection - Batch Processing
        stage_x_frame = ctk.CTkFrame(main_frame)
        stage_x_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(
            stage_x_frame,
            text="Stage X Output JSON Files (x files) - Batch Processing",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=5)
        
        # File selection buttons for Stage X
        buttons_frame_x = ctk.CTkFrame(stage_x_frame)
        buttons_frame_x.pack(fill="x", padx=10, pady=5)
        
        def browse_multiple_stage_x_files():
            filenames = filedialog.askopenfilenames(
                title="Select Stage X output JSON files",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            if filenames:
                if not hasattr(self, 'stage_z_selected_stage_x_files'):
                    self.stage_z_selected_stage_x_files = []
                for filename in filenames:
                    if filename not in self.stage_z_selected_stage_x_files:
                        self.stage_z_selected_stage_x_files.append(filename)
                        self._add_stage_z_stage_x_file_to_ui(filename)
                self._update_stage_z_triples()
        
        def select_folder_stage_x_files():
            folder_path = filedialog.askdirectory(
                title="Select folder containing Stage X output JSON files"
            )
            if not folder_path:
                return
            
            import glob
            json_files = glob.glob(os.path.join(folder_path, "*.json"))
            json_files = [f for f in json_files if os.path.isfile(f)]
            
            if not json_files:
                messagebox.showinfo("Info", "No JSON files found in selected folder")
                return
            
            if not hasattr(self, 'stage_z_selected_stage_x_files'):
                self.stage_z_selected_stage_x_files = []
            
            added_count = 0
            for json_file in json_files:
                if json_file not in self.stage_z_selected_stage_x_files:
                    self.stage_z_selected_stage_x_files.append(json_file)
                    self._add_stage_z_stage_x_file_to_ui(json_file)
                    added_count += 1
            
            if added_count > 0:
                self._update_stage_z_triples()
            messagebox.showinfo("Success", f"Added {added_count} JSON file(s) from folder")
        
        ctk.CTkButton(
            buttons_frame_x,
            text="Browse Multiple Stage X Files",
            command=browse_multiple_stage_x_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        ctk.CTkButton(
            buttons_frame_x,
            text="Select Folder (Stage X)",
            command=select_folder_stage_x_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        # Stage X files list (scrollable)
        list_frame_x = ctk.CTkFrame(stage_x_frame)
        list_frame_x.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            list_frame_x,
            text="Selected Stage X Files:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_z_stage_x_files_list_scroll'):
            self.stage_z_stage_x_files_list_scroll = ctk.CTkScrollableFrame(list_frame_x, height=100)
        self.stage_z_stage_x_files_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize selected files list
        if not hasattr(self, 'stage_z_selected_stage_x_files'):
            self.stage_z_selected_stage_x_files = []
        
        # Stage Y Output Files Selection - Batch Processing
        stage_y_frame = ctk.CTkFrame(main_frame)
        stage_y_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(
            stage_y_frame,
            text="Stage Y Output JSON Files (y files) - Batch Processing",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=5)
        
        # File selection buttons for Stage Y
        buttons_frame_y = ctk.CTkFrame(stage_y_frame)
        buttons_frame_y.pack(fill="x", padx=10, pady=5)
        
        def browse_multiple_stage_y_files():
            filenames = filedialog.askopenfilenames(
                title="Select Stage Y output JSON files",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            if filenames:
                if not hasattr(self, 'stage_z_selected_stage_y_files'):
                    self.stage_z_selected_stage_y_files = []
                for filename in filenames:
                    if filename not in self.stage_z_selected_stage_y_files:
                        self.stage_z_selected_stage_y_files.append(filename)
                        self._add_stage_z_stage_y_file_to_ui(filename)
                self._update_stage_z_triples()
        
        def select_folder_stage_y_files():
            folder_path = filedialog.askdirectory(
                title="Select folder containing Stage Y output JSON files"
            )
            if not folder_path:
                return
            
            import glob
            json_files = glob.glob(os.path.join(folder_path, "*.json"))
            json_files = [f for f in json_files if os.path.isfile(f)]
            
            if not json_files:
                messagebox.showinfo("Info", "No JSON files found in selected folder")
                return
            
            if not hasattr(self, 'stage_z_selected_stage_y_files'):
                self.stage_z_selected_stage_y_files = []
            
            added_count = 0
            for json_file in json_files:
                if json_file not in self.stage_z_selected_stage_y_files:
                    self.stage_z_selected_stage_y_files.append(json_file)
                    self._add_stage_z_stage_y_file_to_ui(json_file)
                    added_count += 1
            
            if added_count > 0:
                self._update_stage_z_triples()
            messagebox.showinfo("Success", f"Added {added_count} JSON file(s) from folder")
        
        ctk.CTkButton(
            buttons_frame_y,
            text="Browse Multiple Stage Y Files",
            command=browse_multiple_stage_y_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        ctk.CTkButton(
            buttons_frame_y,
            text="Select Folder (Stage Y)",
            command=select_folder_stage_y_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        # Stage Y files list (scrollable)
        list_frame_y = ctk.CTkFrame(stage_y_frame)
        list_frame_y.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            list_frame_y,
            text="Selected Stage Y Files:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_z_stage_y_files_list_scroll'):
            self.stage_z_stage_y_files_list_scroll = ctk.CTkScrollableFrame(list_frame_y, height=100)
        self.stage_z_stage_y_files_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize selected files list
        if not hasattr(self, 'stage_z_selected_stage_y_files'):
            self.stage_z_selected_stage_y_files = []
        
        # Triples Section
        triples_frame = ctk.CTkFrame(main_frame)
        triples_frame.pack(fill="x", pady=10)
        
        triples_header_frame = ctk.CTkFrame(triples_frame)
        triples_header_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(
            triples_header_frame,
            text="Triples (Stage A ↔ Stage X ↔ Stage Y):",
            font=ctk.CTkFont(size=14, weight="bold"),
        ).pack(side="left", padx=10, pady=5)
        
        ctk.CTkButton(
            triples_header_frame,
            text="Auto-Triple-Pair",
            command=self._auto_triple_pair_stage_z_files,
            width=150,
            height=30,
            fg_color="green",
            hover_color="darkgreen"
        ).pack(side="right", padx=10, pady=5)
        
        # Triples list (scrollable)
        triples_list_frame = ctk.CTkFrame(triples_frame)
        triples_list_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            triples_list_frame,
            text="File Triples:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'stage_z_triples_list_scroll'):
            self.stage_z_triples_list_scroll = ctk.CTkScrollableFrame(triples_list_frame, height=150)
        self.stage_z_triples_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize triples
        if not hasattr(self, 'stage_z_triples'):
            self.stage_z_triples = []
        if not hasattr(self, 'stage_z_triples_info_list'):
            self.stage_z_triples_info_list = []
        
        # Prompt Section
        prompt_frame = ctk.CTkFrame(main_frame)
        prompt_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(prompt_frame, text="Prompt for RichText Generation:",
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        # Stage Z prompt mode (default vs custom)
        stage_z_mode_frame = ctk.CTkFrame(prompt_frame)
        stage_z_mode_frame.pack(fill="x", padx=10, pady=(0, 5))
        if not hasattr(self, 'stage_z_prompt_type_var'):
            self.stage_z_prompt_type_var = ctk.StringVar(value="default")
        ctk.CTkRadioButton(
            stage_z_mode_frame,
            text="Use Default Prompt",
            variable=self.stage_z_prompt_type_var,
            value="default",
            command=self.on_stage_z_prompt_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)
        ctk.CTkRadioButton(
            stage_z_mode_frame,
            text="Use Custom Prompt",
            variable=self.stage_z_prompt_type_var,
            value="custom",
            command=self.on_stage_z_prompt_type_change,
        ).pack(side="left", padx=(0, 10), pady=5)
        
        # Default Stage Z prompt combobox
        stage_z_default_frame = ctk.CTkFrame(prompt_frame)
        stage_z_default_frame.pack(fill="x", padx=10, pady=(0, 10))
        
        ctk.CTkLabel(
            stage_z_default_frame,
            text="Default RichText Generation Prompt:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", pady=(0, 5))
        
        stage_z_prompt_names = self.prompt_manager.get_prompt_names()
        preferred_stage_z_name = "RichText Generation Prompt"
        if preferred_stage_z_name in stage_z_prompt_names:
            stage_z_default_value = preferred_stage_z_name
        else:
            stage_z_default_value = stage_z_prompt_names[0] if stage_z_prompt_names else ""
        if not hasattr(self, 'stage_z_default_prompt_var'):
            self.stage_z_default_prompt_var = ctk.StringVar(value=stage_z_default_value)
        if not hasattr(self, 'stage_z_default_prompt_combo'):
            self.stage_z_default_prompt_combo = ctk.CTkComboBox(
                stage_z_default_frame,
                values=stage_z_prompt_names,
                variable=self.stage_z_default_prompt_var,
                width=400,
                command=self.on_stage_z_default_prompt_selected,
            )
        self.stage_z_default_prompt_combo.pack(anchor="w", pady=(0, 5))
        
        # Textbox for Stage Z prompt
        if not hasattr(self, 'stage_z_prompt_text'):
            self.stage_z_prompt_text = ctk.CTkTextbox(prompt_frame, height=150, font=self.farsi_text_font)
        self.stage_z_prompt_text.pack(fill="x", padx=10, pady=(0, 5))
        
        # Initialize Stage Z prompt textbox
        self.on_stage_z_prompt_type_change()
        
        # Model Selection
        model_frame = ctk.CTkFrame(main_frame)
        model_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(model_frame, text="Model:",
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_z_model_var'):
            default_model = self.model_var.get() if hasattr(self, 'model_var') else "gemini-2.5-pro"
            self.stage_z_model_var = ctk.StringVar(value=default_model)
        ctk.CTkComboBox(model_frame, values=APIConfig.TEXT_MODELS, variable=self.stage_z_model_var, width=300).pack(anchor="w", padx=10, pady=5)
        
        # Delay Setting
        delay_frame = ctk.CTkFrame(main_frame)
        delay_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(delay_frame, text="Delay Between Triples (seconds):", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_z_delay_var'):
            self.stage_z_delay_var = ctk.StringVar(value="5")
        
        delay_entry_frame = ctk.CTkFrame(delay_frame)
        delay_entry_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(delay_entry_frame, text="Delay:").pack(side="left", padx=5)
        delay_entry = ctk.CTkEntry(delay_entry_frame, textvariable=self.stage_z_delay_var, width=100)
        delay_entry.pack(side="left", padx=5)
        ctk.CTkLabel(delay_entry_frame, text="seconds", text_color="gray").pack(side="left", padx=5)
        
        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)
        
        if not hasattr(self, 'stage_z_process_btn'):
            self.stage_z_process_btn = ctk.CTkButton(
                process_btn_frame,
                text="Process All Triples",
                command=self.process_stage_z_batch,
                font=ctk.CTkFont(size=16, weight="bold"),
                height=40
            )
        self.stage_z_process_btn.pack(pady=10)
        
        # Progress bar for batch processing
        if not hasattr(self, 'stage_z_progress_bar'):
            self.stage_z_progress_bar = ctk.CTkProgressBar(process_btn_frame, width=400)
        self.stage_z_progress_bar.pack(pady=10)
        self.stage_z_progress_bar.set(0)
        
        # Status Label
        if not hasattr(self, 'stage_z_status_label'):
            self.stage_z_status_label = ctk.CTkLabel(
                process_btn_frame,
                text="Ready",
                font=ctk.CTkFont(size=12),
                text_color="gray"
            )
        self.stage_z_status_label.pack(pady=5)
    
    def setup_json_to_csv_ui(self, parent):
        """Setup UI for JSON to CSV Converter"""
        main_frame = ctk.CTkScrollableFrame(parent)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Navigation button
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
        
        title = ctk.CTkLabel(main_frame, text="JSON to CSV Converter", 
                    font=ctk.CTkFont(size=24, weight="bold"))
        title.pack(pady=(0, 20))
        
        desc = ctk.CTkLabel(
            main_frame,
            text="Convert multiple JSON files from all stages to CSV format - Batch Processing.",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        desc.pack(pady=(0, 20))
        
        # JSON Files Selection - Batch Processing
        json_files_frame = ctk.CTkFrame(main_frame)
        json_files_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(
            json_files_frame,
            text="JSON Files - Batch Processing",
            font=ctk.CTkFont(size=14, weight="bold")
        ).pack(anchor="w", padx=10, pady=5)
        
        # File selection buttons
        buttons_frame = ctk.CTkFrame(json_files_frame)
        buttons_frame.pack(fill="x", padx=10, pady=5)
        
        def browse_multiple_json_files():
            filenames = filedialog.askopenfilenames(
                title="Select JSON files",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            if filenames:
                if not hasattr(self, 'json_to_csv_selected_files'):
                    self.json_to_csv_selected_files = []
                for filename in filenames:
                    if filename not in self.json_to_csv_selected_files:
                        self.json_to_csv_selected_files.append(filename)
                        self._add_json_file_to_csv_ui(filename)
        
        def select_folder_json_files():
            folder_path = filedialog.askdirectory(
                title="Select folder containing JSON files"
            )
            if not folder_path:
                return
            
            import glob
            json_files = glob.glob(os.path.join(folder_path, "*.json"))
            json_files = [f for f in json_files if os.path.isfile(f)]
            
            if not json_files:
                messagebox.showinfo("Info", "No JSON files found in selected folder")
                return
            
            if not hasattr(self, 'json_to_csv_selected_files'):
                self.json_to_csv_selected_files = []
            
            added_count = 0
            for json_file in json_files:
                if json_file not in self.json_to_csv_selected_files:
                    self.json_to_csv_selected_files.append(json_file)
                    self._add_json_file_to_csv_ui(json_file)
                    added_count += 1
            
            if added_count > 0:
                messagebox.showinfo("Success", f"Added {added_count} JSON file(s) from folder")
        
        ctk.CTkButton(
            buttons_frame,
            text="Browse Multiple JSON Files",
            command=browse_multiple_json_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        ctk.CTkButton(
            buttons_frame,
            text="Select Folder (JSON)",
            command=select_folder_json_files,
            width=200,
        ).pack(side="left", padx=5, pady=5)
        
        # JSON files list (scrollable)
        list_frame = ctk.CTkFrame(json_files_frame)
        list_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(
            list_frame,
            text="Selected JSON Files:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'json_to_csv_files_list_scroll'):
            self.json_to_csv_files_list_scroll = ctk.CTkScrollableFrame(list_frame, height=150)
        self.json_to_csv_files_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize selected files list
        if not hasattr(self, 'json_to_csv_selected_files'):
            self.json_to_csv_selected_files = []
        
        # Settings Section
        settings_frame = ctk.CTkFrame(main_frame)
        settings_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(settings_frame, text="Conversion Settings",
                    font=ctk.CTkFont(size=16, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        # Delimiter Selection
        delimiter_frame = ctk.CTkFrame(settings_frame)
        delimiter_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(delimiter_frame, text="CSV Delimiter:", width=150).pack(side="left", padx=5)
        
        if not hasattr(self, 'json_to_csv_delimiter_var'):
            self.json_to_csv_delimiter_var = ctk.StringVar(value=";;;")
        
        delimiter_options_frame = ctk.CTkFrame(delimiter_frame)
        delimiter_options_frame.pack(side="left", padx=5)
        
        ctk.CTkRadioButton(
            delimiter_options_frame,
            text=";;; (Default)",
            variable=self.json_to_csv_delimiter_var,
            value=";;;",
            font=ctk.CTkFont(size=11)
        ).pack(side="left", padx=5)
        
        ctk.CTkRadioButton(
            delimiter_options_frame,
            text=", (Comma)",
            variable=self.json_to_csv_delimiter_var,
            value=",",
            font=ctk.CTkFont(size=11)
        ).pack(side="left", padx=5)
        
        ctk.CTkRadioButton(
            delimiter_options_frame,
            text="; (Semicolon)",
            variable=self.json_to_csv_delimiter_var,
            value=";",
            font=ctk.CTkFont(size=11)
        ).pack(side="left", padx=5)
        
        ctk.CTkRadioButton(
            delimiter_options_frame,
            text="Tab",
            variable=self.json_to_csv_delimiter_var,
            value="\t",
            font=ctk.CTkFont(size=11)
        ).pack(side="left", padx=5)
        
        # Output Directory Selection
        output_dir_frame = ctk.CTkFrame(settings_frame)
        output_dir_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(output_dir_frame, text="Output Directory:", width=150).pack(side="left", padx=5)
        
        if not hasattr(self, 'json_to_csv_output_dir_var'):
            self.json_to_csv_output_dir_var = ctk.StringVar(value="")
        
        if not hasattr(self, 'json_to_csv_same_folder_var'):
            self.json_to_csv_same_folder_var = ctk.BooleanVar(value=True)
        
        ctk.CTkCheckBox(
            output_dir_frame,
            text="Save in same folder as JSON files",
            variable=self.json_to_csv_same_folder_var,
            command=self._on_json_to_csv_output_dir_option_change
        ).pack(side="left", padx=5)
        
        output_dir_entry_frame = ctk.CTkFrame(output_dir_frame)
        output_dir_entry_frame.pack(side="left", fill="x", expand=True, padx=5)
        
        if not hasattr(self, 'json_to_csv_output_dir_entry'):
            self.json_to_csv_output_dir_entry = ctk.CTkEntry(
                output_dir_entry_frame,
                textvariable=self.json_to_csv_output_dir_var,
                state="disabled"
            )
        self.json_to_csv_output_dir_entry.pack(side="left", fill="x", expand=True, padx=5)
        
        ctk.CTkButton(
            output_dir_entry_frame,
            text="Browse",
            command=self._browse_json_to_csv_output_dir,
            width=100
        ).pack(side="left", padx=5)
        
        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)
        
        if not hasattr(self, 'json_to_csv_process_btn'):
            self.json_to_csv_process_btn = ctk.CTkButton(
                process_btn_frame,
                text="Convert All to CSV",
                command=self.convert_json_to_csv_batch,
                font=ctk.CTkFont(size=16, weight="bold"),
                height=40
            )
        self.json_to_csv_process_btn.pack(pady=10)
        
        # Progress bar for batch processing
        if not hasattr(self, 'json_to_csv_progress_bar'):
            self.json_to_csv_progress_bar = ctk.CTkProgressBar(process_btn_frame, width=400)
        self.json_to_csv_progress_bar.pack(pady=10)
        self.json_to_csv_progress_bar.set(0)
        
        # Status Label
        if not hasattr(self, 'json_to_csv_status_label'):
            self.json_to_csv_status_label = ctk.CTkLabel(
                process_btn_frame,
                text="Ready",
                font=ctk.CTkFont(size=12),
                text_color="gray"
            )
        self.json_to_csv_status_label.pack(pady=5)
        
        # Results list (scrollable)
        results_frame = ctk.CTkFrame(main_frame)
        results_frame.pack(fill="both", expand=True, pady=10)
        
        ctk.CTkLabel(
            results_frame,
            text="Conversion Results:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        if not hasattr(self, 'json_to_csv_results_list_scroll'):
            self.json_to_csv_results_list_scroll = ctk.CTkScrollableFrame(results_frame, height=150)
        self.json_to_csv_results_list_scroll.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Initialize results list
        if not hasattr(self, 'json_to_csv_results_list'):
            self.json_to_csv_results_list = []
    
    def _add_stage_z_stage_a_file_to_ui(self, file_path: str):
        """Add a Stage A file to the UI list"""
        file_frame = ctk.CTkFrame(self.stage_z_stage_a_files_list_scroll)
        file_frame.pack(fill="x", padx=5, pady=2)
        
        file_name = os.path.basename(file_path)
        
        name_label = ctk.CTkLabel(
            file_frame,
            text=file_name,
            font=ctk.CTkFont(size=11),
            anchor="w"
        )
        name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
        
        def remove_file():
            if file_path in self.stage_z_selected_stage_a_files:
                self.stage_z_selected_stage_a_files.remove(file_path)
            file_frame.destroy()
            self._update_stage_z_triples()
        
        remove_btn = ctk.CTkButton(
            file_frame,
            text="✕",
            command=remove_file,
            width=30,
            height=25,
            fg_color="red",
            hover_color="darkred"
        )
        remove_btn.pack(side="right", padx=5, pady=5)
    
    def _add_stage_z_stage_x_file_to_ui(self, file_path: str):
        """Add a Stage X file to the UI list"""
        file_frame = ctk.CTkFrame(self.stage_z_stage_x_files_list_scroll)
        file_frame.pack(fill="x", padx=5, pady=2)
        
        file_name = os.path.basename(file_path)
        
        name_label = ctk.CTkLabel(
            file_frame,
            text=file_name,
            font=ctk.CTkFont(size=11),
            anchor="w"
        )
        name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
        
        def remove_file():
            if file_path in self.stage_z_selected_stage_x_files:
                self.stage_z_selected_stage_x_files.remove(file_path)
            file_frame.destroy()
            self._update_stage_z_triples()
        
        remove_btn = ctk.CTkButton(
            file_frame,
            text="✕",
            command=remove_file,
            width=30,
            height=25,
            fg_color="red",
            hover_color="darkred"
        )
        remove_btn.pack(side="right", padx=5, pady=5)
    
    def _add_stage_z_stage_y_file_to_ui(self, file_path: str):
        """Add a Stage Y file to the UI list"""
        file_frame = ctk.CTkFrame(self.stage_z_stage_y_files_list_scroll)
        file_frame.pack(fill="x", padx=5, pady=2)
        
        file_name = os.path.basename(file_path)
        
        name_label = ctk.CTkLabel(
            file_frame,
            text=file_name,
            font=ctk.CTkFont(size=11),
            anchor="w"
        )
        name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
        
        def remove_file():
            if file_path in self.stage_z_selected_stage_y_files:
                self.stage_z_selected_stage_y_files.remove(file_path)
            file_frame.destroy()
            self._update_stage_z_triples()
        
        remove_btn = ctk.CTkButton(
            file_frame,
            text="✕",
            command=remove_file,
            width=30,
            height=25,
            fg_color="red",
            hover_color="darkred"
        )
        remove_btn.pack(side="right", padx=5, pady=5)
    
    def _extract_book_chapter_from_stage_a_for_z(self, stage_a_path: str):
        """Extract book and chapter from Stage A file (from PointId or filename)"""
        try:
            # Try to load Stage A and extract from PointId
            data = json.load(open(stage_a_path, 'r', encoding='utf-8'))
            records = data.get("data") or data.get("rows", [])
            if records and records[0].get("PointId"):
                point_id = records[0].get("PointId")
                if isinstance(point_id, str) and len(point_id) >= 6:
                    book_id = int(point_id[0:3])
                    chapter_id = int(point_id[3:6])
                    return book_id, chapter_id
        except:
            pass
        
        # Fallback: try to extract from filename
        try:
            basename = os.path.basename(stage_a_path)
            name_without_ext = os.path.splitext(basename)[0]
            import re
            # Try pattern: a{book}{chapter}+{name}
            match = re.match(r'^a(\d{3})(\d{3})\+', name_without_ext)
            if match:
                book_id = int(match.group(1))
                chapter_id = int(match.group(2))
                return book_id, chapter_id
            # Try pattern: a{book}{chapter}
            if name_without_ext.startswith('a') and len(name_without_ext) >= 7:
                book_chapter = name_without_ext[1:]
                book_id = int(book_chapter[0:3])
                chapter_id = int(book_chapter[3:6])
                return book_id, chapter_id
        except:
            pass
        
        return None, None
    
    def _extract_book_chapter_from_stage_x_for_z(self, stage_x_path: str):
        """Extract book and chapter from Stage X output file (from metadata or filename)"""
        try:
            # Try to load Stage X and extract from metadata
            data = json.load(open(stage_x_path, 'r', encoding='utf-8'))
            metadata = self.get_metadata_from_json(data)
            book_id = metadata.get("book_id")
            chapter_id = metadata.get("chapter_id")
            if book_id is not None and chapter_id is not None:
                return book_id, chapter_id
        except:
            pass
        
        # Fallback: try to extract from filename
        try:
            basename = os.path.basename(stage_x_path)
            name_without_ext = os.path.splitext(basename)[0]
            import re
            # Try pattern: x{book}{chapter}+{name}
            match = re.match(r'^x(\d{3})(\d{3})\+', name_without_ext)
            if match:
                book_id = int(match.group(1))
                chapter_id = int(match.group(2))
                return book_id, chapter_id
            # Try pattern: x{book}{chapter}
            if name_without_ext.startswith('x') and len(name_without_ext) >= 7:
                book_chapter = name_without_ext[1:]
                book_id = int(book_chapter[0:3])
                chapter_id = int(book_chapter[3:6])
                return book_id, chapter_id
        except:
            pass
        
        return None, None
    
    def _extract_book_chapter_from_stage_y_for_z(self, stage_y_path: str):
        """Extract book and chapter from Stage Y output file (from metadata or filename)"""
        try:
            # Try to load Stage Y and extract from metadata
            data = json.load(open(stage_y_path, 'r', encoding='utf-8'))
            metadata = self.get_metadata_from_json(data)
            book_id = metadata.get("book_id")
            chapter_id = metadata.get("chapter_id")
            if book_id is not None and chapter_id is not None:
                return book_id, chapter_id
        except:
            pass
        
        # Fallback: try to extract from filename
        try:
            basename = os.path.basename(stage_y_path)
            name_without_ext = os.path.splitext(basename)[0]
            import re
            # Try pattern: y{book}{chapter}+{name}
            match = re.match(r'^y(\d{3})(\d{3})\+', name_without_ext)
            if match:
                book_id = int(match.group(1))
                chapter_id = int(match.group(2))
                return book_id, chapter_id
            # Try pattern: y{book}{chapter}
            if name_without_ext.startswith('y') and len(name_without_ext) >= 7:
                book_chapter = name_without_ext[1:]
                book_id = int(book_chapter[0:3])
                chapter_id = int(book_chapter[3:6])
                return book_id, chapter_id
        except:
            pass
        
        return None, None
    
    def _auto_triple_pair_stage_z_files(self):
        """Auto-pair Stage A, Stage X, and Stage Y files based on Book/Chapter"""
        if not hasattr(self, 'stage_z_selected_stage_a_files') or not self.stage_z_selected_stage_a_files:
            messagebox.showwarning("Warning", "Please add at least one Stage A file")
            return
        
        if not hasattr(self, 'stage_z_selected_stage_x_files') or not self.stage_z_selected_stage_x_files:
            messagebox.showwarning("Warning", "Please add at least one Stage X file")
            return
        
        if not hasattr(self, 'stage_z_selected_stage_y_files') or not self.stage_z_selected_stage_y_files:
            messagebox.showwarning("Warning", "Please add at least one Stage Y file")
            return
        
        # Build dictionaries: {(book_id, chapter_id): file_path}
        stage_a_dict = {}
        stage_x_dict = {}
        stage_y_dict = {}
        
        for stage_a_path in self.stage_z_selected_stage_a_files:
            book_id, chapter_id = self._extract_book_chapter_from_stage_a_for_z(stage_a_path)
            if book_id is not None and chapter_id is not None:
                key = (book_id, chapter_id)
                stage_a_dict[key] = stage_a_path
        
        for stage_x_path in self.stage_z_selected_stage_x_files:
            book_id, chapter_id = self._extract_book_chapter_from_stage_x_for_z(stage_x_path)
            if book_id is not None and chapter_id is not None:
                key = (book_id, chapter_id)
                stage_x_dict[key] = stage_x_path
        
        for stage_y_path in self.stage_z_selected_stage_y_files:
            book_id, chapter_id = self._extract_book_chapter_from_stage_y_for_z(stage_y_path)
            if book_id is not None and chapter_id is not None:
                key = (book_id, chapter_id)
                stage_y_dict[key] = stage_y_path
        
        # Find common Book/Chapter keys (all three files must exist)
        common_keys = set(stage_a_dict.keys()) & set(stage_x_dict.keys()) & set(stage_y_dict.keys())
        
        if not common_keys:
            messagebox.showwarning(
                "Warning",
                "No matching Book/Chapter found across all three file types. "
                "Please ensure that Stage A, Stage X, and Stage Y files have matching Book/Chapter IDs."
            )
            return
        
        # Create triples
        triples = []
        for key in sorted(common_keys):
            book_id, chapter_id = key
            triple = {
                'stage_a_path': stage_a_dict[key],
                'stage_x_path': stage_x_dict[key],
                'stage_y_path': stage_y_dict[key],
                'book_id': book_id,
                'chapter_id': chapter_id,
                'status': 'pending',
                'output_path': None,
                'error': None
            }
            triples.append(triple)
        
        self.stage_z_triples = triples
        self._update_stage_z_triples_ui()
        
        triple_count = len(triples)
        messagebox.showinfo(
            "Auto-Triple-Pairing Complete",
            f"Created {triple_count} triple(s) for Book/Chapter combinations"
        )
    
    def _update_stage_z_triples(self):
        """Update triples when files are added/removed"""
        # Clear existing triples UI
        for widget in self.stage_z_triples_list_scroll.winfo_children():
            widget.destroy()
        
        self.stage_z_triples_info_list = []
        
        # Re-pair if we have triples
        if hasattr(self, 'stage_z_triples') and self.stage_z_triples:
            self._update_stage_z_triples_ui()
    
    def _update_stage_z_triples_ui(self):
        """Update the triples UI display"""
        # Clear existing triples UI
        for widget in self.stage_z_triples_list_scroll.winfo_children():
            widget.destroy()
        
        self.stage_z_triples_info_list = []
        
        if not hasattr(self, 'stage_z_triples') or not self.stage_z_triples:
            return
        
        for triple in self.stage_z_triples:
            triple_frame = ctk.CTkFrame(self.stage_z_triples_list_scroll)
            triple_frame.pack(fill="x", padx=5, pady=2)
            
            stage_a_name = os.path.basename(triple['stage_a_path']) if triple['stage_a_path'] else "None"
            stage_x_name = os.path.basename(triple['stage_x_path']) if triple['stage_x_path'] else "None"
            stage_y_name = os.path.basename(triple['stage_y_path']) if triple['stage_y_path'] else "None"
            
            triple_text = f"{stage_a_name} ↔ {stage_x_name} ↔ {stage_y_name}"
            
            # Main triple info
            info_frame = ctk.CTkFrame(triple_frame)
            info_frame.pack(fill="x", padx=5, pady=2)
            
            name_label = ctk.CTkLabel(
                info_frame,
                text=triple_text,
                font=ctk.CTkFont(size=11),
                anchor="w"
            )
            name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
            
            status_label = ctk.CTkLabel(
                info_frame,
                text=triple.get('status', 'pending').upper(),
                font=ctk.CTkFont(size=10),
                text_color="gray" if triple.get('status') == 'pending' else 
                          "green" if triple.get('status') == 'completed' else "red"
            )
            status_label.pack(side="right", padx=10, pady=5)
            
            self.stage_z_triples_info_list.append({
                'triple': triple,
                'status_label': status_label,
                'frame': triple_frame
            })
    
    def process_stage_z_batch(self):
        """Process multiple Stage A + Stage X + Stage Y triples for Stage Z"""
        def worker():
            try:
                if not hasattr(self, 'stage_z_triples') or not self.stage_z_triples:
                    self.root.after(0, lambda: messagebox.showwarning(
                        "Warning",
                        "Please add files and create triples first. Click 'Auto-Triple-Pair' button."
                    ))
                    return
                
                # Filter triples that have all three files
                valid_triples = [t for t in self.stage_z_triples 
                               if t['stage_a_path'] and t['stage_x_path'] and t['stage_y_path']]
                
                if not valid_triples:
                    self.root.after(0, lambda: messagebox.showwarning(
                        "Warning",
                        "No valid triples found. Each triple must have Stage A, Stage X, and Stage Y files."
                    ))
                    return
                
                self.stage_z_process_btn.configure(
                    state="disabled",
                    text="Processing Batch..."
                )
                
                # Get prompt
                prompt = self.stage_z_prompt_text.get("1.0", tk.END).strip()
                if not prompt:
                    default_prompt = self.prompt_manager.get_prompt("RichText Generation Prompt")
                    if default_prompt:
                        prompt = default_prompt
                        self.logger.info("Using default RichText generation prompt from prompts.json")
                    else:
                        self.root.after(0, lambda: messagebox.showerror("Error", "Please enter a RichText generation prompt"))
                        return
                
                # Get model
                model_name = self.stage_z_model_var.get() if hasattr(self, 'stage_z_model_var') else "gemini-2.5-pro"
                
                # Get delay
                try:
                    delay_seconds = float(self.stage_z_delay_var.get() if hasattr(self, 'stage_z_delay_var') else "5")
                    if delay_seconds < 0:
                        delay_seconds = 0
                except:
                    delay_seconds = 5
                
                total_triples = len(valid_triples)
                completed = 0
                failed = 0
                
                # Reset progress bar
                self.root.after(0, lambda: self.stage_z_progress_bar.set(0))
                
                # Process each triple
                for idx, triple in enumerate(valid_triples):
                    stage_a_path = triple['stage_a_path']
                    stage_x_path = triple['stage_x_path']
                    stage_y_path = triple['stage_y_path']
                    
                    stage_a_name = os.path.basename(stage_a_path)
                    stage_x_name = os.path.basename(stage_x_path)
                    stage_y_name = os.path.basename(stage_y_path)
                    
                    # Update status to processing
                    if hasattr(self, 'stage_z_triples_info_list'):
                        for triple_info in self.stage_z_triples_info_list:
                            if triple_info['triple'] == triple:
                                self.root.after(0, lambda sl=triple_info['status_label']:
                                               sl.configure(text="PROCESSING", text_color="blue"))
                                break
                    
                    # Update progress label
                    self.root.after(0, lambda idx=idx, total=total_triples, sa=stage_a_name, sx=stage_x_name, sy=stage_y_name:
                                   self.stage_z_status_label.configure(
                                       text=f"Processing triple {idx+1}/{total}: {sa} ↔ {sx} ↔ {sy}",
                                       text_color="blue"
                                   ))
                    
                    # Progress bar
                    progress = idx / total_triples
                    self.root.after(0, lambda p=progress: self.stage_z_progress_bar.set(p))
                    
                    try:
                        def progress_callback(msg: str):
                            self.root.after(0, lambda m=msg:
                                           self.stage_z_status_label.configure(text=m))
                        
                        # Process Stage Z
                        output_path = self.stage_z_processor.process_stage_z(
                            stage_a_path=stage_a_path,
                            stage_x_output_path=stage_x_path,
                            stage_y_output_path=stage_y_path,
                            prompt=prompt,
                            model_name=model_name,
                            output_dir=self.get_default_output_dir(stage_a_path),
                            progress_callback=progress_callback
                        )
                        
                        if output_path and os.path.exists(output_path):
                            completed += 1
                            triple['output_path'] = output_path
                            triple['status'] = 'completed'
                            
                            # Update status to completed
                            if hasattr(self, 'stage_z_triples_info_list'):
                                for triple_info in self.stage_z_triples_info_list:
                                    if triple_info['triple'] == triple:
                                        self.root.after(0, lambda sl=triple_info['status_label']:
                                                       sl.configure(text="COMPLETED", text_color="green"))
                                        break
                            
                            self.logger.info(
                                f"Successfully processed: {stage_a_name} ↔ {stage_x_name} ↔ {stage_y_name} -> {os.path.basename(output_path)}"
                            )
                        else:
                            failed += 1
                            triple['status'] = 'failed'
                            triple['error'] = "Processing returned no output"
                            
                            # Update status to failed
                            if hasattr(self, 'stage_z_triples_info_list'):
                                for triple_info in self.stage_z_triples_info_list:
                                    if triple_info['triple'] == triple:
                                        self.root.after(0, lambda sl=triple_info['status_label']:
                                                       sl.configure(text="FAILED", text_color="red"))
                                        break
                            
                            self.logger.error(f"Failed to process: {stage_a_name} ↔ {stage_x_name} ↔ {stage_y_name}")
                            
                    except Exception as e:
                        failed += 1
                        triple['status'] = 'failed'
                        triple['error'] = str(e)
                        self.logger.error(f"Error processing {stage_a_name} ↔ {stage_x_name} ↔ {stage_y_name}: {str(e)}", exc_info=True)
                        
                        # Update status to failed
                        if hasattr(self, 'stage_z_triples_info_list'):
                            for triple_info in self.stage_z_triples_info_list:
                                if triple_info['triple'] == triple:
                                    self.root.after(0, lambda sl=triple_info['status_label']:
                                                   sl.configure(text="FAILED", text_color="red"))
                                    break
                    
                    # Delay before next batch (except for the last one)
                    if idx < total_triples - 1 and delay_seconds > 0:
                        self.root.after(0, lambda: self.stage_z_status_label.configure(
                            text=f"Waiting {delay_seconds} seconds before next batch..."))
                        import time
                        time.sleep(delay_seconds)
                
                # Final progress update
                self.root.after(0, lambda: self.stage_z_progress_bar.set(1.0))
                
                self.root.after(0, lambda: self.stage_z_status_label.configure(
                    text=f"Batch completed: {completed} succeeded, {failed} failed",
                    text_color="green" if failed == 0 else "orange"
                ))
                
                # Show summary
                self.root.after(0, lambda: messagebox.showinfo(
                    "Batch Processing Complete",
                    f"Stage Z batch completed!\n\n"
                    f"Total triples: {total_triples}\n"
                    f"Successful: {completed}\n"
                    f"Failed: {failed}"
                ))
            
            except Exception as e:
                self.logger.error(f"Error in batch Stage Z processing: {str(e)}", exc_info=True)
                self.root.after(0, lambda: messagebox.showerror(
                    "Error",
                    f"Batch processing error:\n{str(e)}"
                ))
            finally:
                self.root.after(0, lambda: self.stage_z_process_btn.configure(
                    state="normal",
                    text="Process All Triples"
                ))
        
        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def _add_stage_x_pdf_file_to_ui(self, file_path: str):
        """Add a PDF file to the UI list"""
        file_frame = ctk.CTkFrame(self.stage_x_pdf_files_list_scroll)
        file_frame.pack(fill="x", padx=5, pady=2)
        
        file_name = os.path.basename(file_path)
        
        name_label = ctk.CTkLabel(
            file_frame,
            text=file_name,
            font=ctk.CTkFont(size=11),
            anchor="w"
        )
        name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
        
        def remove_file():
            if file_path in self.stage_x_selected_pdf_files:
                self.stage_x_selected_pdf_files.remove(file_path)
            file_frame.destroy()
            self._update_stage_x_pairs()
        
        remove_btn = ctk.CTkButton(
            file_frame,
            text="✕",
            command=remove_file,
            width=30,
            height=25,
            fg_color="red",
            hover_color="darkred"
        )
        remove_btn.pack(side="right", padx=5, pady=5)
    
    def _add_stage_x_stage_a_file_to_ui(self, file_path: str):
        """Add a Stage A file to the UI list"""
        file_frame = ctk.CTkFrame(self.stage_x_stage_a_files_list_scroll)
        file_frame.pack(fill="x", padx=5, pady=2)
        
        file_name = os.path.basename(file_path)
        
        name_label = ctk.CTkLabel(
            file_frame,
            text=file_name,
            font=ctk.CTkFont(size=11),
            anchor="w"
        )
        name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
        
        def remove_file():
            if file_path in self.stage_x_selected_stage_a_files:
                self.stage_x_selected_stage_a_files.remove(file_path)
            file_frame.destroy()
            self._update_stage_x_pairs()
        
        remove_btn = ctk.CTkButton(
            file_frame,
            text="✕",
            command=remove_file,
            width=30,
            height=25,
            fg_color="red",
            hover_color="darkred"
        )
        remove_btn.pack(side="right", padx=5, pady=5)
    
    def _extract_book_chapter_from_stage_a_for_x(self, stage_a_path: str):
        """Extract book and chapter from Stage A file (from PointId or filename)"""
        try:
            # Try to load Stage A and extract from PointId
            data = json.load(open(stage_a_path, 'r', encoding='utf-8'))
            records = data.get("data") or data.get("rows", [])
            if records and records[0].get("PointId"):
                point_id = records[0].get("PointId")
                if isinstance(point_id, str) and len(point_id) >= 6:
                    book_id = int(point_id[0:3])
                    chapter_id = int(point_id[3:6])
                    return book_id, chapter_id
        except:
            pass
        
        # Fallback: try to extract from filename (a{book}{chapter}.json or a{book}{chapter}+{name}.json)
        try:
            basename = os.path.basename(stage_a_path)
            name_without_ext = os.path.splitext(basename)[0]
            import re
            # Try pattern: a{book}{chapter}+{name}
            match = re.match(r'^a(\d{3})(\d{3})\+', name_without_ext)
            if match:
                book_id = int(match.group(1))
                chapter_id = int(match.group(2))
                return book_id, chapter_id
            # Try pattern: a{book}{chapter}
            if name_without_ext.startswith('a') and len(name_without_ext) >= 7:
                book_chapter = name_without_ext[1:]
                book_id = int(book_chapter[0:3])
                chapter_id = int(book_chapter[3:6])
                return book_id, chapter_id
        except:
            pass
        
        return None, None
    
    def _auto_pair_stage_x_files(self):
        """Auto-pair PDF files with Stage A files"""
        pairing_mode = self.stage_x_pairing_mode_var.get() if hasattr(self, 'stage_x_pairing_mode_var') else "common"
        
        if not hasattr(self, 'stage_x_selected_pdf_files') or not self.stage_x_selected_pdf_files:
            messagebox.showwarning("Warning", "Please add at least one PDF file")
            return
        
        if not hasattr(self, 'stage_x_selected_stage_a_files') or not self.stage_x_selected_stage_a_files:
            messagebox.showwarning("Warning", "Please add at least one Stage A file")
            return
        
        pairs = []
        
        if pairing_mode == "common":
            # Use first PDF for all Stage A files
            common_pdf = self.stage_x_selected_pdf_files[0]
            for stage_a_path in self.stage_x_selected_stage_a_files:
                book_id, chapter_id = self._extract_book_chapter_from_stage_a_for_x(stage_a_path)
                pair = {
                    'pdf_path': common_pdf,
                    'stage_a_path': stage_a_path,
                    'book_id': book_id,
                    'chapter_id': chapter_id,
                    'status': 'pending',
                    'output_path': None,
                    'error': None
                }
                pairs.append(pair)
        else:
            # Pair by Book/Chapter (if multiple PDFs, try to match)
            # For now, use first PDF for all (can be enhanced later)
            if len(self.stage_x_selected_pdf_files) == 1:
                common_pdf = self.stage_x_selected_pdf_files[0]
                for stage_a_path in self.stage_x_selected_stage_a_files:
                    book_id, chapter_id = self._extract_book_chapter_from_stage_a_for_x(stage_a_path)
                    pair = {
                        'pdf_path': common_pdf,
                        'stage_a_path': stage_a_path,
                        'book_id': book_id,
                        'chapter_id': chapter_id,
                        'status': 'pending',
                        'output_path': None,
                        'error': None
                    }
                    pairs.append(pair)
            else:
                # Multiple PDFs: try to pair by Book/Chapter
                paired_pdfs = set()
                for stage_a_path in self.stage_x_selected_stage_a_files:
                    book_id, chapter_id = self._extract_book_chapter_from_stage_a_for_x(stage_a_path)
                    
                    if book_id is None or chapter_id is None:
                        # Can't extract book/chapter, use first PDF
                        matched_pdf = self.stage_x_selected_pdf_files[0]
                    else:
                        # Try to find matching PDF (for now, use first available)
                        matched_pdf = None
                        for pdf_path in self.stage_x_selected_pdf_files:
                            if pdf_path not in paired_pdfs:
                                matched_pdf = pdf_path
                                paired_pdfs.add(pdf_path)
                                break
                        
                        if not matched_pdf:
                            matched_pdf = self.stage_x_selected_pdf_files[0]
                    
                    pair = {
                        'pdf_path': matched_pdf,
                        'stage_a_path': stage_a_path,
                        'book_id': book_id,
                        'chapter_id': chapter_id,
                        'status': 'pending',
                        'output_path': None,
                        'error': None
                    }
                    pairs.append(pair)
        
        self.stage_x_pairs = pairs
        self._update_stage_x_pairs_ui()
        
        paired_count = len(pairs)
        messagebox.showinfo(
            "Auto-Pairing Complete",
            f"Created {paired_count} pair(s)"
        )
    
    def _update_stage_x_pairs(self):
        """Update pairs when files are added/removed"""
        # Clear existing pairs UI
        for widget in self.stage_x_pairs_list_scroll.winfo_children():
            widget.destroy()
        
        self.stage_x_pairs_info_list = []
        
        # Re-pair if we have pairs
        if hasattr(self, 'stage_x_pairs') and self.stage_x_pairs:
            self._update_stage_x_pairs_ui()
    
    def _update_stage_x_pairs_ui(self):
        """Update the pairs UI display"""
        # Clear existing pairs UI
        for widget in self.stage_x_pairs_list_scroll.winfo_children():
            widget.destroy()
        
        self.stage_x_pairs_info_list = []
        
        if not hasattr(self, 'stage_x_pairs') or not self.stage_x_pairs:
            return
        
        for pair in self.stage_x_pairs:
            pair_frame = ctk.CTkFrame(self.stage_x_pairs_list_scroll)
            pair_frame.pack(fill="x", padx=5, pady=2)
            
            pdf_name = os.path.basename(pair['pdf_path']) if pair['pdf_path'] else "None"
            stage_a_name = os.path.basename(pair['stage_a_path']) if pair['stage_a_path'] else "None"
            
            pair_text = f"{pdf_name} ↔ {stage_a_name}"
            
            # Main pair info
            info_frame = ctk.CTkFrame(pair_frame)
            info_frame.pack(fill="x", padx=5, pady=2)
            
            name_label = ctk.CTkLabel(
                info_frame,
                text=pair_text,
                font=ctk.CTkFont(size=11),
                anchor="w"
            )
            name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
            
            status_label = ctk.CTkLabel(
                info_frame,
                text=pair.get('status', 'pending').upper(),
                font=ctk.CTkFont(size=10),
                text_color="gray" if pair.get('status') == 'pending' else 
                          "green" if pair.get('status') == 'completed' else "red"
            )
            status_label.pack(side="right", padx=10, pady=5)
            
            self.stage_x_pairs_info_list.append({
                'pair': pair,
                'status_label': status_label,
                'frame': pair_frame
            })
    
    def process_stage_x_batch(self):
        """Process multiple PDF + Stage A pairs for Stage X"""
        def worker():
            try:
                if not hasattr(self, 'stage_x_pairs') or not self.stage_x_pairs:
                    self.root.after(0, lambda: messagebox.showwarning(
                        "Warning",
                        "Please add files and create pairs first. Click 'Auto-Pair' button."
                    ))
                    return
                
                # Filter pairs that have both PDF and Stage A file
                valid_pairs = [p for p in self.stage_x_pairs if p['pdf_path'] and p['stage_a_path']]
                
                if not valid_pairs:
                    self.root.after(0, lambda: messagebox.showwarning(
                        "Warning",
                        "No valid pairs found. Each pair must have both PDF and Stage A file."
                    ))
                    return
                
                self.stage_x_process_btn.configure(
                    state="disabled",
                    text="Processing Batch..."
                )
                
                # Get prompts
                pdf_prompt = self.stage_x_pdf_prompt_text.get("1.0", tk.END).strip()
                if not pdf_prompt:
                    default_pdf_prompt = self.prompt_manager.get_prompt("OCR Extraction Prompt")
                    if default_pdf_prompt:
                        pdf_prompt = default_pdf_prompt
                        self.logger.info("Using default PDF extraction prompt from prompts.json")
                    else:
                        self.root.after(0, lambda: messagebox.showerror("Error", "Please enter a PDF extraction prompt"))
                        return
                
                change_prompt = self.stage_x_change_prompt_text.get("1.0", tk.END).strip()
                if not change_prompt:
                    default_change_prompt = self.prompt_manager.get_prompt("Change Detection Prompt")
                    if default_change_prompt:
                        change_prompt = default_change_prompt
                        self.logger.info("Using default change detection prompt from prompts.json")
                    else:
                        self.root.after(0, lambda: messagebox.showerror("Error", "Please enter a change detection prompt"))
                        return
                
                # Get models
                pdf_model = self.stage_x_pdf_model_var.get() if hasattr(self, 'stage_x_pdf_model_var') else "gemini-2.5-pro"
                change_model = self.stage_x_change_model_var.get() if hasattr(self, 'stage_x_change_model_var') else "gemini-2.5-pro"
                
                # Get delay
                try:
                    delay_seconds = float(self.stage_x_delay_var.get() if hasattr(self, 'stage_x_delay_var') else "5")
                    if delay_seconds < 0:
                        delay_seconds = 0
                except:
                    delay_seconds = 5
                
                total_pairs = len(valid_pairs)
                completed = 0
                failed = 0
                
                # PDF extraction cache: {pdf_path: extracted_json_path}
                pdf_extraction_cache = {}
                
                # Reset progress bar
                self.root.after(0, lambda: self.stage_x_progress_bar.set(0))
                
                # Process each pair
                for idx, pair in enumerate(valid_pairs):
                    pdf_path = pair['pdf_path']
                    stage_a_path = pair['stage_a_path']
                    
                    pdf_name = os.path.basename(pdf_path)
                    stage_a_name = os.path.basename(stage_a_path)
                    
                    # Update status to processing
                    if hasattr(self, 'stage_x_pairs_info_list'):
                        for pair_info in self.stage_x_pairs_info_list:
                            if pair_info['pair'] == pair:
                                self.root.after(0, lambda sl=pair_info['status_label']:
                                               sl.configure(text="PROCESSING", text_color="blue"))
                                break
                    
                    # Update progress label
                    self.root.after(0, lambda idx=idx, total=total_pairs, pdf=pdf_name, sa=stage_a_name:
                                   self.stage_x_status_label.configure(
                                       text=f"Processing pair {idx+1}/{total}: {pdf} ↔ {sa}",
                                       text_color="blue"
                                   ))
                    
                    # Progress bar
                    progress = idx / total_pairs
                    self.root.after(0, lambda p=progress: self.stage_x_progress_bar.set(p))
                    
                    try:
                        def progress_callback(msg: str):
                            self.root.after(0, lambda m=msg:
                                           self.stage_x_status_label.configure(text=m))
                        
                        # Check cache for PDF extraction
                        if pdf_path not in pdf_extraction_cache:
                            # Part 1: Extract PDF (one time per PDF)
                            self.root.after(0, lambda: self.stage_x_status_label.configure(
                                text=f"Extracting PDF: {pdf_name}..."))
                            
                            # Determine output directory for PDF extraction
                            pdf_output_dir = self.get_default_output_dir(stage_a_path)
                            
                            extracted_path = self.stage_x_processor._extract_pdf_with_txt_saving(
                                pdf_path=pdf_path,
                                prompt=pdf_prompt,
                                model_name=pdf_model,
                                output_dir=pdf_output_dir,
                                progress_callback=progress_callback
                            )
                            
                            if not extracted_path or not os.path.exists(extracted_path):
                                raise Exception(f"PDF extraction failed for {pdf_name}")
                            
                            pdf_extraction_cache[pdf_path] = extracted_path
                            self.logger.info(f"Cached PDF extraction: {pdf_path} -> {extracted_path}")
                        else:
                            extracted_path = pdf_extraction_cache[pdf_path]
                            self.logger.info(f"Using cached PDF extraction: {pdf_path}")
                        
                        # Part 2: Detect changes
                        self.root.after(0, lambda: self.stage_x_status_label.configure(
                            text=f"Detecting changes: {stage_a_name}..."))
                        
                        # Process Stage X (use cached extracted path if available)
                        output_path = self.stage_x_processor.process_stage_x(
                            old_book_pdf_path=pdf_path,
                            pdf_extraction_prompt=pdf_prompt,
                            pdf_extraction_model=pdf_model,
                            stage_a_path=stage_a_path,
                            changes_prompt=change_prompt,
                            changes_model=change_model,
                            output_dir=self.get_default_output_dir(stage_a_path),
                            progress_callback=progress_callback,
                            pdf_extracted_path=extracted_path  # Use cached extraction
                        )
                        
                        if output_path and os.path.exists(output_path):
                            completed += 1
                            pair['output_path'] = output_path
                            pair['status'] = 'completed'
                            
                            # Update status to completed
                            if hasattr(self, 'stage_x_pairs_info_list'):
                                for pair_info in self.stage_x_pairs_info_list:
                                    if pair_info['pair'] == pair:
                                        self.root.after(0, lambda sl=pair_info['status_label']:
                                                       sl.configure(text="COMPLETED", text_color="green"))
                                        break
                            
                            self.logger.info(
                                f"Successfully processed: {pdf_name} ↔ {stage_a_name} -> {os.path.basename(output_path)}"
                            )
                        else:
                            failed += 1
                            pair['status'] = 'failed'
                            pair['error'] = "Processing returned no output"
                            
                            # Update status to failed
                            if hasattr(self, 'stage_x_pairs_info_list'):
                                for pair_info in self.stage_x_pairs_info_list:
                                    if pair_info['pair'] == pair:
                                        self.root.after(0, lambda sl=pair_info['status_label']:
                                                       sl.configure(text="FAILED", text_color="red"))
                                        break
                            
                            self.logger.error(f"Failed to process: {pdf_name} ↔ {stage_a_name}")
                            
                    except Exception as e:
                        failed += 1
                        pair['status'] = 'failed'
                        pair['error'] = str(e)
                        self.logger.error(f"Error processing {pdf_name} ↔ {stage_a_name}: {str(e)}", exc_info=True)
                        
                        # Update status to failed
                        if hasattr(self, 'stage_x_pairs_info_list'):
                            for pair_info in self.stage_x_pairs_info_list:
                                if pair_info['pair'] == pair:
                                    self.root.after(0, lambda sl=pair_info['status_label']:
                                                   sl.configure(text="FAILED", text_color="red"))
                                    break
                    
                    # Delay before next batch (except for the last one)
                    if idx < total_pairs - 1 and delay_seconds > 0:
                        self.root.after(0, lambda: self.stage_x_status_label.configure(
                            text=f"Waiting {delay_seconds} seconds before next batch..."))
                        import time
                        time.sleep(delay_seconds)
                
                # Final progress update
                self.root.after(0, lambda: self.stage_x_progress_bar.set(1.0))
                
                self.root.after(0, lambda: self.stage_x_status_label.configure(
                    text=f"Batch completed: {completed} succeeded, {failed} failed",
                    text_color="green" if failed == 0 else "orange"
                ))
                
                # Show summary
                self.root.after(0, lambda: messagebox.showinfo(
                    "Batch Processing Complete",
                    f"Stage X batch completed!\n\n"
                    f"Total pairs: {total_pairs}\n"
                    f"Successful: {completed}\n"
                    f"Failed: {failed}"
                ))
            
            except Exception as e:
                self.logger.error(f"Error in batch Stage X processing: {str(e)}", exc_info=True)
                self.root.after(0, lambda: messagebox.showerror(
                    "Error",
                    f"Batch processing error:\n{str(e)}"
                ))
            finally:
                self.root.after(0, lambda: self.stage_x_process_btn.configure(
                    state="normal",
                    text="Process All Pairs"
                ))
        
        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def process_stage_x(self):
        """Process Stage X (redirects to batch processing for backward compatibility)"""
        # Redirect to batch processing
        self.process_stage_x_batch()
    
    def process_stage_y(self):
        """Process Stage Y (redirects to batch processing for backward compatibility)"""
        # Redirect to batch processing
        self.process_stage_y_batch()
    
    def process_stage_z(self):
        """Process Stage Z (redirects to batch processing for backward compatibility)"""
        # Redirect to batch processing
        self.process_stage_z_batch()
    
    def _add_json_file_to_csv_ui(self, file_path: str):
        """Add a JSON file to the UI list"""
        file_frame = ctk.CTkFrame(self.json_to_csv_files_list_scroll)
        file_frame.pack(fill="x", padx=5, pady=2)
        
        file_name = os.path.basename(file_path)
        
        name_label = ctk.CTkLabel(
            file_frame,
            text=file_name,
            font=ctk.CTkFont(size=11),
            anchor="w"
        )
        name_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
        
        def remove_file():
            if file_path in self.json_to_csv_selected_files:
                self.json_to_csv_selected_files.remove(file_path)
            file_frame.destroy()
        
        remove_btn = ctk.CTkButton(
            file_frame,
            text="✕",
            command=remove_file,
            width=30,
            height=25,
            fg_color="red",
            hover_color="darkred"
        )
        remove_btn.pack(side="right", padx=5, pady=5)
    
    def _on_json_to_csv_output_dir_option_change(self):
        """Handle output directory option change"""
        if self.json_to_csv_same_folder_var.get():
            self.json_to_csv_output_dir_entry.configure(state="disabled")
            self.json_to_csv_output_dir_var.set("")
        else:
            self.json_to_csv_output_dir_entry.configure(state="normal")
    
    def _browse_json_to_csv_output_dir(self):
        """Browse for output directory"""
        folder_path = filedialog.askdirectory(
            title="Select output directory for CSV files"
        )
        if folder_path:
            self.json_to_csv_output_dir_var.set(folder_path)
            self.json_to_csv_same_folder_var.set(False)
            self.json_to_csv_output_dir_entry.configure(state="normal")
    
    def convert_json_to_csv_file(self, json_file_path: str, output_csv_path: str, delimiter: str = ";;;") -> bool:
        """
        Convert JSON file to CSV file with comprehensive support for multiple JSON structures.
        
        Supports:
        - Direct arrays: [{...}, {...}]
        - Objects with data/points/rows: {metadata: {...}, data: [...]}
        - Nested structures: {metadata: {...}, chapters: [{subchapters: [{topics: [{extractions: [...]}]}]}]}
        
        Args:
            json_file_path: Path to JSON file
            output_csv_path: Path to output CSV file
            delimiter: CSV delimiter (default: ";;;")
            
        Returns:
            True if successful, False otherwise
        """
        def flatten_nested_structure(data, parent_key: str = "", separator: str = "_"):
            """
            Flatten nested structures (chapters -> subchapters -> topics -> extractions) into flat rows.
            
            Args:
                data: The data structure to flatten
                parent_key: Parent key for nested structures
                separator: Separator for nested keys
                
            Returns:
                List of flattened dictionaries
            """
            rows = []
            
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        # Check if this is a nested structure with chapters/subchapters/topics/extractions
                        if "chapters" in item:
                            # Process chapters
                            for chapter in item.get("chapters", []):
                                chapter_name = chapter.get("chapter", "")
                                
                                # Process subchapters
                                for subchapter in chapter.get("subchapters", []):
                                    subchapter_name = subchapter.get("subchapter", "")
                                    
                                    # Process topics
                                    for topic in subchapter.get("topics", []):
                                        topic_name = topic.get("topic", "")
                                        
                                        # Process extractions
                                        for extraction in topic.get("extractions", []):
                                            row = {
                                                "chapter": chapter_name,
                                                "subchapter": subchapter_name,
                                                "topic": topic_name,
                                                **extraction
                                            }
                                            rows.append(row)
                                        
                                        # If topic has no extractions but has other data, add it
                                        if not topic.get("extractions") and topic:
                                            row = {
                                                "chapter": chapter_name,
                                                "subchapter": subchapter_name,
                                                "topic": topic_name,
                                                **{k: v for k, v in topic.items() if k != "extractions"}
                                            }
                                            rows.append(row)
                                    
                                    # If subchapter has no topics but has other data
                                    if not subchapter.get("topics") and subchapter:
                                        row = {
                                            "chapter": chapter_name,
                                            "subchapter": subchapter_name,
                                            **{k: v for k, v in subchapter.items() if k != "topics"}
                                        }
                                        rows.append(row)
                                
                                # If chapter has no subchapters but has other data
                                if not chapter.get("subchapters") and chapter:
                                    row = {
                                        "chapter": chapter_name,
                                        **{k: v for k, v in chapter.items() if k != "subchapters"}
                                    }
                                    rows.append(row)
                        else:
                            # Regular list item, add as is
                            rows.append(item)
            elif isinstance(data, dict):
                # Single dictionary, return as list
                return [data]
            
            return rows
        
        def extract_rows_from_json(json_data):
            """
            Extract rows from JSON data supporting multiple structures.
            
            Args:
                json_data: JSON data (dict or list)
                
            Returns:
                List of data records
            """
            # If json_data is already a list, check if it needs flattening
            if isinstance(json_data, list):
                # Check if first item has nested structure
                if json_data and isinstance(json_data[0], dict) and "chapters" in json_data[0]:
                    return flatten_nested_structure(json_data)
                return json_data
            
            # If json_data is a dict, look for data/points/rows/chapters keys
            if isinstance(json_data, dict):
                # Check for nested chapters structure first
                if "chapters" in json_data:
                    chapters_data = json_data["chapters"]
                    if isinstance(chapters_data, list):
                        # Check if it's a nested structure
                        if chapters_data and isinstance(chapters_data[0], dict) and "subchapters" in chapters_data[0]:
                            return flatten_nested_structure(chapters_data)
                        return chapters_data
                    elif isinstance(chapters_data, dict):
                        # Single chapter dict, try to extract rows/data from it
                        if "rows" in chapters_data:
                            return chapters_data["rows"]
                        elif "data" in chapters_data:
                            return chapters_data["data"]
                        else:
                            return [chapters_data]
                
                # Check for standard keys
                if "data" in json_data:
                    data = json_data["data"]
                    if isinstance(data, list) and data and isinstance(data[0], dict) and "chapters" in data[0]:
                        return flatten_nested_structure(data)
                    return data if isinstance(data, list) else [data]
                
                elif "points" in json_data:
                    return json_data["points"] if isinstance(json_data["points"], list) else [json_data["points"]]
                
                elif "rows" in json_data:
                    return json_data["rows"] if isinstance(json_data["rows"], list) else [json_data["rows"]]
                
                else:
                    # Try using BaseStageProcessor method if available
                    if hasattr(self, 'stage_z_processor'):
                        rows = self.stage_z_processor.get_data_from_json(json_data)
                        if rows:
                            return rows
                    elif hasattr(self, 'stage_e_processor'):
                        rows = self.stage_e_processor.get_data_from_json(json_data)
                        if rows:
                            return rows
                    
                    # No recognized key, return empty list
                    return []
            
            return []
        
        try:
            # Load JSON file
            with open(json_file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            # Extract rows from JSON
            rows = extract_rows_from_json(json_data)
            
            if not rows:
                self.logger.warning(f"No data rows found in {json_file_path}")
                return False
            
            # Filter out non-dict items
            rows = [row for row in rows if isinstance(row, dict)]
            
            if not rows:
                self.logger.error(f"Invalid JSON format: no valid dictionary rows found in {json_file_path}")
                return False
            
            # Get all unique headers from all rows and normalize case-sensitive duplicates
            all_headers_dict = {}  # lowercase_key -> original_key (most common case)
            header_counts = {}  # lowercase_key -> {original_key: count}
            
            for row in rows:
                for key in row.keys():
                    key_lower = key.lower()
                    if key_lower not in header_counts:
                        header_counts[key_lower] = {}
                    if key not in header_counts[key_lower]:
                        header_counts[key_lower][key] = 0
                    header_counts[key_lower][key] += 1
            
            # For each lowercase key, choose the most common original case
            for key_lower, variants in header_counts.items():
                # Choose the variant with highest count, or first alphabetically if tie
                most_common = max(variants.items(), key=lambda x: (x[1], x[0]))
                all_headers_dict[key_lower] = most_common[0]
            
            headers = sorted(list(all_headers_dict.values()))
            
            if not headers:
                self.logger.error(f"No headers found in {json_file_path}")
                return False
            
            # Create mapping from original keys to normalized keys (build once, use many times)
            key_mapping = {}
            for key_lower, normalized_key in all_headers_dict.items():
                # Find all original keys that map to this normalized key
                for original_key in header_counts[key_lower].keys():
                    if original_key != normalized_key:
                        key_mapping[original_key] = normalized_key
            
            # Normalize all rows to use consistent key casing
            normalized_rows = []
            for row in rows:
                normalized_row = {}
                for key, value in row.items():
                    normalized_key = key_mapping.get(key, key)
                    # If normalized key already exists in this row, merge values (prefer non-empty)
                    if normalized_key in normalized_row:
                        if not normalized_row[normalized_key] and value:
                            normalized_row[normalized_key] = value
                        # If both have values, prefer the one from the normalized key
                    else:
                        normalized_row[normalized_key] = value
                normalized_rows.append(normalized_row)
            
            rows = normalized_rows
            
            # Build CSV content
            csv_lines = []
            
            # Add header row
            csv_lines.append(delimiter.join(headers))
            
            # Add data rows
            for row in rows:
                csv_line = delimiter.join(str(row.get(h, "")) for h in headers)
                csv_lines.append(csv_line)
            
            csv_text = "\n".join(csv_lines)
            
            # Save CSV file
            os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
            with open(output_csv_path, 'w', encoding='utf-8') as f:
                f.write(csv_text)
            
            self.logger.info(f"Successfully converted {json_file_path} to {output_csv_path} (Total rows: {len(rows)}, Total columns: {len(headers)})")
            return True
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON file {json_file_path}: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error converting {json_file_path} to CSV: {str(e)}", exc_info=True)
            return False
    
    def convert_json_to_csv_batch(self):
        """Convert multiple JSON files to CSV format"""
        def worker():
            try:
                if not hasattr(self, 'json_to_csv_selected_files') or not self.json_to_csv_selected_files:
                    self.root.after(0, lambda: messagebox.showwarning(
                        "Warning",
                        "Please add at least one JSON file to convert."
                    ))
                    return
                
                self.json_to_csv_process_btn.configure(
                    state="disabled",
                    text="Converting..."
                )
                
                # Get delimiter
                delimiter = self.json_to_csv_delimiter_var.get() if hasattr(self, 'json_to_csv_delimiter_var') else ";;;"
                
                # Get output directory option
                use_same_folder = self.json_to_csv_same_folder_var.get() if hasattr(self, 'json_to_csv_same_folder_var') else True
                custom_output_dir = self.json_to_csv_output_dir_var.get().strip() if hasattr(self, 'json_to_csv_output_dir_var') else ""
                
                # Validate custom output directory if specified
                if not use_same_folder and custom_output_dir:
                    if not os.path.exists(custom_output_dir):
                        self.root.after(0, lambda: messagebox.showerror(
                            "Error",
                            f"Output directory does not exist:\n{custom_output_dir}"
                        ))
                        return
                
                total_files = len(self.json_to_csv_selected_files)
                completed = 0
                failed = 0
                
                # Clear previous results
                for widget in self.json_to_csv_results_list_scroll.winfo_children():
                    widget.destroy()
                self.json_to_csv_results_list = []
                
                # Reset progress bar
                self.root.after(0, lambda: self.json_to_csv_progress_bar.set(0))
                
                # Process each file
                for idx, json_file_path in enumerate(self.json_to_csv_selected_files):
                    file_name = os.path.basename(json_file_path)
                    
                    # Update progress label
                    self.root.after(0, lambda idx=idx, total=total_files, fn=file_name:
                                   self.json_to_csv_status_label.configure(
                                       text=f"Converting {idx+1}/{total}: {fn}",
                                       text_color="blue"
                                   ))
                    
                    # Progress bar
                    progress = idx / total_files
                    self.root.after(0, lambda p=progress: self.json_to_csv_progress_bar.set(p))
                    
                    try:
                        # Determine output CSV path
                        json_basename = os.path.splitext(os.path.basename(json_file_path))[0]
                        csv_filename = f"{json_basename}.csv"
                        
                        if use_same_folder:
                            # Save in same folder as JSON file
                            output_csv_path = os.path.join(os.path.dirname(json_file_path), csv_filename)
                        else:
                            # Save in custom output directory
                            if custom_output_dir:
                                output_csv_path = os.path.join(custom_output_dir, csv_filename)
                            else:
                                # Fallback to same folder
                                output_csv_path = os.path.join(os.path.dirname(json_file_path), csv_filename)
                        
                        # Convert JSON to CSV
                        success = self.convert_json_to_csv_file(
                            json_file_path=json_file_path,
                            output_csv_path=output_csv_path,
                            delimiter=delimiter
                        )
                        
                        if success:
                            completed += 1
                            result = {
                                'json_file': json_file_path,
                                'csv_file': output_csv_path,
                                'status': 'success'
                            }
                            self.json_to_csv_results_list.append(result)
                            
                            # Add to UI
                            result_frame = ctk.CTkFrame(self.json_to_csv_results_list_scroll)
                            result_frame.pack(fill="x", padx=5, pady=2)
                            
                            result_text = f"Success: {file_name} -> {csv_filename}"
                            result_label = ctk.CTkLabel(
                                result_frame,
                                text=result_text,
                                font=ctk.CTkFont(size=11),
                                anchor="w",
                                text_color="green"
                            )
                            result_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
                            
                            self.logger.info(f"Successfully converted: {file_name} → {csv_filename}")
                        else:
                            failed += 1
                            result = {
                                'json_file': json_file_path,
                                'csv_file': None,
                                'status': 'failed'
                            }
                            self.json_to_csv_results_list.append(result)
                            
                            # Add to UI
                            result_frame = ctk.CTkFrame(self.json_to_csv_results_list_scroll)
                            result_frame.pack(fill="x", padx=5, pady=2)
                            
                            result_text = f"Failed: {file_name} (Failed)"
                            result_label = ctk.CTkLabel(
                                result_frame,
                                text=result_text,
                                font=ctk.CTkFont(size=11),
                                anchor="w",
                                text_color="red"
                            )
                            result_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
                            
                            self.logger.error(f"Failed to convert: {file_name}")
                            
                    except Exception as e:
                        failed += 1
                        self.logger.error(f"Error converting {file_name}: {str(e)}", exc_info=True)
                        
                        # Add error to UI
                        result_frame = ctk.CTkFrame(self.json_to_csv_results_list_scroll)
                        result_frame.pack(fill="x", padx=5, pady=2)
                        
                        result_text = f"Error: {file_name} (Error: {str(e)[:50]})"
                        result_label = ctk.CTkLabel(
                            result_frame,
                            text=result_text,
                            font=ctk.CTkFont(size=11),
                            anchor="w",
                            text_color="red"
                        )
                        result_label.pack(side="left", padx=10, pady=5, fill="x", expand=True)
                
                # Final progress update
                self.root.after(0, lambda: self.json_to_csv_progress_bar.set(1.0))
                
                self.root.after(0, lambda: self.json_to_csv_status_label.configure(
                    text=f"Conversion completed: {completed} succeeded, {failed} failed",
                    text_color="green" if failed == 0 else "orange"
                ))
                
                # Show summary
                self.root.after(0, lambda: messagebox.showinfo(
                    "Conversion Complete",
                    f"JSON to CSV conversion completed!\n\n"
                    f"Total files: {total_files}\n"
                    f"Successful: {completed}\n"
                    f"Failed: {failed}"
                ))
            
            except Exception as e:
                self.logger.error(f"Error in batch JSON to CSV conversion: {str(e)}", exc_info=True)
                self.root.after(0, lambda: messagebox.showerror(
                    "Error",
                    f"Batch conversion error:\n{str(e)}"
                ))
            finally:
                self.root.after(0, lambda: self.json_to_csv_process_btn.configure(
                    state="normal",
                    text="Convert All to CSV"
                ))
        
        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def start_automated_pipeline_from_main(self):
        """Start automated pipeline from main page"""
        # Validate PDF is selected
        if not self.pdf_path or not os.path.exists(self.pdf_path):
            messagebox.showerror("Error", "Please select a PDF file first.")
            return
        
        # Validate PointID
        pointid = self.auto_pipeline_pointid_var.get().strip()
        if not pointid or len(pointid) != 10 or not pointid.isdigit():
            messagebox.showerror("Error", "Please enter a valid 10-digit PointID (e.g., 1050030001).")
            return
        
        # Validate chapter name
        chapter_name = self.auto_pipeline_chapter_var.get().strip()
        if not chapter_name:
            messagebox.showerror("Error", "Please enter chapter name.")
            return
        
        # Get Stage 1 prompt from main page
        if self.use_custom_prompt:
            stage1_prompt = self.custom_prompt
        else:
            if not self.selected_prompt_name:
                messagebox.showerror("Error", "Please select or enter a prompt for Stage 1.")
                return
            stage1_prompt = self.prompt_manager.get_prompt(self.selected_prompt_name) or ""
        
        if not stage1_prompt:
            messagebox.showerror("Error", "Please select or enter a prompt for Stage 1.")
            return
        
        # Get model (use default from main view)
        stage1_model = self.get_default_model()
        
        # Disable button
        self.auto_pipeline_start_btn.configure(state="disabled", text="Running Pipeline...")
        
        # Run in background
        threading.Thread(target=self.run_full_automated_pipeline_worker, daemon=True).start()
    
    def run_full_automated_pipeline_worker(self):
        """Background worker for full automated pipeline starting from PDF"""
        try:
            # Get all inputs
            pdf_path = self.pdf_path
            pointid = self.auto_pipeline_pointid_var.get().strip()
            chapter_name = self.auto_pipeline_chapter_var.get().strip()
            word_file_path = self.auto_pipeline_word_var.get().strip()
            if not word_file_path or not os.path.exists(word_file_path):
                word_file_path = None
            
            # Get Stage 1 prompt and model
            if self.use_custom_prompt:
                stage1_prompt = self.custom_prompt
            else:
                stage1_prompt = self.prompt_manager.get_prompt(self.selected_prompt_name) or ""
            
            stage1_model = self.get_default_model()
            
            # Get Stage 2, 3, 4 prompts from prompt_manager
            stage2_prompt = self.prompt_manager.get_prompt("Document Processing Prompt") or ""
            stage3_prompt = self.prompt_manager.get_prompt("Stage 3 - Chunked JSON Completion") or ""
            stage4_prompt = self.prompt_manager.get_prompt("Stage 4 - Prompt") or ""
            
            # Get Stage E, J, V prompts
            stage_e_prompt = self.prompt_manager.get_prompt("Image Notes Prompt") or ""
            stage_j_prompt = self.prompt_manager.get_prompt("Importance & Type Prompt") or ""
            stage_v_prompt_1 = self.prompt_manager.get_prompt("Test Bank Generation - Step 1 Prompt") or ""
            stage_v_prompt_2 = self.prompt_manager.get_prompt("Test Bank Generation - Step 2 Prompt") or ""
            stage_v_prompt_3 = self.prompt_manager.get_prompt("Test Bank Generation - Step 3 Prompt") or ""
            
            # Get Stage X, Y, Z prompts (optional)
            old_book_pdf_path = self.auto_pipeline_old_pdf_var.get().strip()
            if not old_book_pdf_path or not os.path.exists(old_book_pdf_path):
                old_book_pdf_path = None
            
            stage_x_pdf_prompt = self.prompt_manager.get_prompt("Stage X - PDF Extraction Prompt") or ""
            stage_x_change_prompt = self.prompt_manager.get_prompt("Change Detection Prompt") or ""
            stage_y_prompt = self.prompt_manager.get_prompt("Deletion Detection Prompt") or ""
            stage_z_prompt = self.prompt_manager.get_prompt("RichText Generation Prompt") or ""
            
            # Validate required prompts
            if not stage2_prompt:
                messagebox.showerror("Error", "Stage 2 prompt not found in prompts.json. Please add it.")
                return
            if not stage3_prompt:
                messagebox.showerror("Error", "Stage 3 prompt not found in prompts.json. Please add it.")
                return
            if not stage4_prompt:
                messagebox.showerror("Error", "Stage 4 prompt not found in prompts.json. Please add it.")
                return
            if not stage_e_prompt:
                messagebox.showerror("Error", "Stage E prompt not found in prompts.json. Please add it.")
                return
            
            # Progress callback
            def progress_callback(msg: str):
                self.root.after(0, lambda: self.auto_pipeline_progress_label.configure(text=msg))
            
            # Initialize orchestrator
            orchestrator = AutomatedPipelineOrchestrator(self.api_client)
            
            # Output directory (use default from main view)
            output_dir = self.get_default_output_dir(pdf_path)
            
            # Run pipeline
            results = orchestrator.run_automated_pipeline(
                pdf_path=pdf_path,
                stage1_prompt=stage1_prompt,
                stage1_model=stage1_model,
                stage2_prompt=stage2_prompt,
                stage2_model=stage1_model,  # Use same model
                chapter_name=chapter_name,
                stage3_prompt=stage3_prompt,
                stage3_model=stage1_model,
                stage4_prompt=stage4_prompt,
                stage4_model=stage1_model,
                start_pointid=pointid,
                stage_e_prompt=stage_e_prompt,
                stage_e_model=stage1_model,
                word_file_path=word_file_path,
                stage_j_prompt=stage_j_prompt if stage_j_prompt else None,
                stage_j_model=stage1_model if stage_j_prompt else None,
                stage_v_prompt_1=stage_v_prompt_1 if stage_v_prompt_1 else None,
                stage_v_model_1=stage1_model if stage_v_prompt_1 else None,
                stage_v_prompt_2=stage_v_prompt_2 if stage_v_prompt_2 else None,
                stage_v_model_2=stage1_model if stage_v_prompt_2 else None,
                stage_v_prompt_3=stage_v_prompt_3 if stage_v_prompt_3 else None,
                stage_v_model_3=stage1_model if stage_v_prompt_3 else None,
                old_book_pdf_path=old_book_pdf_path,
                stage_x_pdf_extraction_prompt=stage_x_pdf_prompt if stage_x_pdf_prompt else None,
                stage_x_pdf_extraction_model=stage1_model if stage_x_pdf_prompt else None,
                stage_x_change_prompt=stage_x_change_prompt if stage_x_change_prompt else None,
                stage_x_change_model=stage1_model if stage_x_change_prompt else None,
                stage_y_prompt=stage_y_prompt if stage_y_prompt else None,
                stage_y_model=stage1_model if stage_y_prompt else None,
                stage_z_prompt=stage_z_prompt if stage_z_prompt else None,
                stage_z_model=stage1_model if stage_z_prompt else None,
                output_dir=output_dir,
                progress_callback=progress_callback
            )
            
            # Show results
            successful = [name for name, result in results.items() if result.status.value == "success"]
            failed = [name for name, result in results.items() if result.status.value == "failed"]
            
            summary = f"Pipeline completed!\n\nSuccessful stages: {', '.join(successful) if successful else 'None'}\n"
            if failed:
                summary += f"\nFailed stages: {', '.join(failed)}\n"
                for name in failed:
                    summary += f"  - Stage {name}: {results[name].error_message}\n"
            
            messagebox.showinfo("Pipeline Complete", summary)
            self.root.after(0, lambda: self.auto_pipeline_progress_label.configure(
                text=f"Completed! Successful: {len(successful)}, Failed: {len(failed)}"
            ))
            
        except Exception as e:
            self.logger.error(f"Error in automated pipeline: {e}", exc_info=True)
            messagebox.showerror("Error", f"Pipeline error:\n{str(e)}")
            self.root.after(0, lambda: self.auto_pipeline_progress_label.configure(
                text=f"Error: {str(e)}", text_color="red"
            ))
        finally:
            self.root.after(0, lambda: self.auto_pipeline_start_btn.configure(
                state="normal", text="Start Automated Pipeline"
            ))
    
    def run(self):
        """Start the GUI main loop"""
        self.root.mainloop()


def main():
    """Main entry point for the application"""
    app = ContentAutomationGUI()
    app.run()


if __name__ == "__main__":
    main()
