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
            text="Extract topics from PDF before OCR extraction. Output: t{book}{chapter}.json",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        desc.pack(pady=(0, 20))
        
        # PDF file selection for Pre-OCR
        pdf_frame = ctk.CTkFrame(main_frame)
        pdf_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(pdf_frame, text="PDF File:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'pre_ocr_pdf_file_var'):
            self.pre_ocr_pdf_file_var = ctk.StringVar()
        
        entry_frame_pdf = ctk.CTkFrame(pdf_frame)
        entry_frame_pdf.pack(fill="x", padx=10, pady=5)
        
        pre_ocr_pdf_entry = ctk.CTkEntry(entry_frame_pdf, textvariable=self.pre_ocr_pdf_file_var)
        pre_ocr_pdf_entry.pack(side="left", fill="x", expand=True, padx=5)
        
        ctk.CTkButton(entry_frame_pdf, text="Browse", 
                     command=self.browse_pre_ocr_pdf_file, 
                     width=80).pack(side="right", padx=5)
        
        # Validation indicator
        if not hasattr(self, 'pre_ocr_pdf_valid'):
            self.pre_ocr_pdf_valid = ctk.CTkLabel(entry_frame_pdf, text="", width=30)
        self.pre_ocr_pdf_valid.pack(side="right", padx=5)
        
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
        
        if not hasattr(self, 'pre_ocr_process_btn'):
            self.pre_ocr_process_btn = ctk.CTkButton(process_frame, text="Extract Topics", 
                                                    command=self.process_pre_ocr_topic, width=200, height=40,
                                                    font=ctk.CTkFont(size=14, weight="bold"))
        self.pre_ocr_process_btn.pack(side="left", padx=10, pady=10)
        
        # Status label
        if not hasattr(self, 'pre_ocr_status_label'):
            self.pre_ocr_status_label = ctk.CTkLabel(process_frame, text="Ready", 
                                                     font=ctk.CTkFont(size=12), text_color="gray")
        self.pre_ocr_status_label.pack(side="left", padx=10, pady=10)
        
        # Output file path
        output_frame = ctk.CTkFrame(main_frame)
        output_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(output_frame, text="Output File (t{book}{chapter}.json):", 
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
    
    
    def browse_pre_ocr_pdf_file(self):
        """Browse for PDF file for Pre-OCR Topic Extraction"""
        filename = filedialog.askopenfilename(
            title="Select PDF File for Pre-OCR Topic Extraction",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")]
        )
        if filename:
            self.pre_ocr_pdf_file_var.set(filename)
            self.pre_ocr_status_label.configure(text=f"PDF selected: {os.path.basename(filename)}", text_color="green")
    
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

    def process_pre_ocr_topic(self):
        """Process Pre-OCR Topic Extraction using LLM"""
        def worker():
            try:
                # Disable process button
                self.pre_ocr_process_btn.configure(state="disabled", text="Processing...")
                self.pre_ocr_status_label.configure(text="Processing Pre-OCR Topic Extraction...", text_color="blue")
                
                # Validate inputs
                pdf_path = self.pre_ocr_pdf_file_var.get().strip()
                if not pdf_path or not os.path.exists(pdf_path):
                    self.pre_ocr_status_label.configure(text="Error: Please select a valid PDF file", text_color="red")
                    messagebox.showerror("Error", "Please select a valid PDF file")
                    return
                
                prompt = self.pre_ocr_prompt_text.get("1.0", tk.END).strip()
                if not prompt:
                    self.pre_ocr_status_label.configure(text="Error: Please enter a prompt", text_color="red")
                    messagebox.showerror("Error", "Please enter a prompt for topic extraction")
                    return
                
                # Validate API keys are loaded
                if not self.api_key_manager.api_keys:
                    if not self.api_key_file_var.get():
                        self.pre_ocr_status_label.configure(text="Error: Please load API keys", text_color="red")
                        messagebox.showerror("Error", "Please load API keys from CSV file")
                        return
                    else:
                        if not self.api_key_manager.load_from_csv(self.api_key_file_var.get()):
                            self.pre_ocr_status_label.configure(text="Error: Failed to load API keys", text_color="red")
                            messagebox.showerror("Error", "Failed to load API keys from CSV file")
                            return
                
                model_name = self.pre_ocr_model_var.get()
                
                def progress_callback(msg: str):
                    self.root.after(0, lambda: self.pre_ocr_status_label.configure(text=msg))
                
                # Process Pre-OCR Topic Extraction
                output_path = self.pre_ocr_topic_processor.process_pre_ocr_topic(
                    pdf_path=pdf_path,
                    prompt=prompt,
                    model_name=model_name,
                    output_dir=self.get_default_output_dir(pdf_path),
                    progress_callback=progress_callback
                )
                
                if output_path and os.path.exists(output_path):
                    self.pre_ocr_output_var.set(output_path)
                    self.pre_ocr_status_label.configure(
                        text=f"Success! Output saved to: {os.path.basename(output_path)}", 
                        text_color="green"
                    )
                    messagebox.showinfo("Success", f"Pre-OCR Topic Extraction completed!\nOutput: {output_path}")
                else:
                    self.pre_ocr_status_label.configure(text="Error: Processing failed", text_color="red")
                    messagebox.showerror("Error", "Pre-OCR Topic Extraction failed. Check logs for details.")
                
            except Exception as e:
                error_msg = f"Error: {str(e)}"
                self.pre_ocr_status_label.configure(text=error_msg, text_color="red")
                self.logger.error(f"Pre-OCR Topic Extraction error: {e}", exc_info=True)
                messagebox.showerror("Error", f"Pre-OCR Topic Extraction failed: {str(e)}")
            finally:
                self.pre_ocr_process_btn.configure(state="normal", text="Extract Topics")
        
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
        
        # JSON selection section
        json_frame = ctk.CTkFrame(main_frame)
        json_frame.pack(fill="x", pady=(0, 20))
        
        ctk.CTkLabel(
            json_frame,
            text="Input JSON (OCR Extraction JSON)",
            font=ctk.CTkFont(size=18, weight="bold"),
        ).pack(pady=(15, 10))
        
        file_frame = ctk.CTkFrame(json_frame)
        file_frame.pack(fill="x", padx=15, pady=5)
        
        ctk.CTkLabel(
            file_frame,
            text="OCR Extraction JSON File:",
            font=ctk.CTkFont(size=12, weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        
        # Only create if doesn't exist
        if not hasattr(self, 'document_processing_ocr_json_var'):
            self.document_processing_ocr_json_var = ctk.StringVar()
            # Try to auto-fill from last OCR extraction path if available
            if hasattr(self, 'last_ocr_extraction_path') and self.last_ocr_extraction_path:
                self.document_processing_ocr_json_var.set(self.last_ocr_extraction_path)
        
        json_entry = ctk.CTkEntry(file_frame, textvariable=self.document_processing_ocr_json_var, width=400)
        json_entry.pack(side="left", fill="x", expand=True, padx=(10, 5), pady=(0, 5))
        
        def browse_ocr_json_file():
            filename = filedialog.askopenfilename(
                title="Select OCR Extraction JSON file",
                filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
            )
            if filename:
                self.document_processing_ocr_json_var.set(filename)
        
        ctk.CTkButton(
            file_frame,
            text="Browse",
            command=browse_ocr_json_file,
            width=80,
        ).pack(side="right", padx=(5, 10), pady=(0, 5))
        
        ctk.CTkLabel(
            file_frame,
            text="This JSON file should have the structure: chapters -> subchapters -> topics -> extractions",
            font=ctk.CTkFont(size=10),
            text_color="gray",
        ).pack(anchor="w", padx=10, pady=(0, 10))
        
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
            text="PointId Settings",
            font=ctk.CTkFont(size=18, weight="bold"),
        ).pack(pady=(15, 10))
        
        # Start PointId
        pointid_row = ctk.CTkFrame(pointid_frame)
        pointid_row.pack(fill="x", padx=15, pady=(5, 10))
        
        ctk.CTkLabel(
            pointid_row,
            text="Start PointId (e.g. 1050030001):",
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
            text="Document Processing Progress",
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
        
        # Full pipeline button
        if not hasattr(self, 'full_pipeline_cancel'):
            self.full_pipeline_cancel = False
        
        def start_full_pipeline():
            full_btn.configure(state="disabled", text="Running Document Processing...")
            self.full_pipeline_cancel = False
            threading.Thread(
                target=self.process_full_pipeline_worker,
                args=(self.root, full_btn),
                daemon=True,
            ).start()
        
        full_btn = ctk.CTkButton(
            controls_frame,
            text="Run Document Processing",
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

    def process_full_pipeline_worker(self, parent_window, start_button):
        """
        Background worker: run Document Processing using OCR Extraction JSON.
        Uses:
          - OCR Extraction JSON (with chapters->subchapters->topics structure)
          - Document Processing prompt (with {Topic_NAME} and {Subchapter_Name} placeholders)
          - Model from main settings
          - Start PointId for the whole chapter
        """
        try:
            # --- Validate and load basic inputs ---
            ocr_json_path = self.document_processing_ocr_json_var.get().strip()
            if not ocr_json_path:
                messagebox.showerror("Error", "Please select the OCR Extraction JSON file.")
                return

            if not os.path.exists(ocr_json_path):
                messagebox.showerror("Error", f"OCR Extraction JSON file not found:\n{ocr_json_path}")
                return

            # Document Processing prompt (should contain {Topic_NAME} and {Subchapter_Name})
            document_prompt = self.second_stage_prompt_text.get("1.0", tk.END).strip()
            if not document_prompt:
                messagebox.showerror("Error", "Please enter a prompt for Document Processing (should contain {Topic_NAME} and {Subchapter_Name} placeholders).")
                return

            # Check if prompt contains placeholders
            if "{Topic_NAME}" not in document_prompt or "{Subchapter_Name}" not in document_prompt:
                self.logger.warning("Document Processing prompt may not contain {Topic_NAME} and {Subchapter_Name} placeholders")

            # Always use default model from main view settings
            model_name = self.get_default_model()

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

            # Use new Document Processing method
            self.update_status("Starting Document Processing with OCR Extraction JSON...")
            
            def progress_callback(message: str):
                self.update_status(message)
            
            final_output_path = self.multi_part_post_processor.process_document_processing_from_ocr_json(
                ocr_json_path=ocr_json_path,
                user_prompt=document_prompt,
                model_name=model_name,
                book_id=book_id,
                chapter_id=chapter_id_num,
                start_point_index=current_index,
                progress_callback=progress_callback,
            )
            
            if not final_output_path or not os.path.exists(final_output_path):
                self.update_status("Document Processing failed.")
                messagebox.showerror("Error", "Document Processing failed. Check logs for details.")
                return
            
            self.update_status(f"✓ Document Processing completed successfully: {os.path.basename(final_output_path)}")
            
            # Load and display final output
            try:
                with open(final_output_path, "r", encoding="utf-8") as f:
                    final_data = json.load(f)
                
                total_points = final_data.get("metadata", {}).get("total_points", 0)
                messagebox.showinfo(
                    "Success",
                    f"Document Processing completed successfully!\n\n"
                    f"Output file: {os.path.basename(final_output_path)}\n"
                    f"Total points: {total_points}\n"
                    f"Next free index: {final_data.get('metadata', {}).get('next_free_index', current_index)}"
                )
                
                # Store path for future use
                self.last_document_processing_path = final_output_path
                
            except Exception as e:
                self.logger.error(f"Error loading final output: {str(e)}")
                messagebox.showerror("Error", f"Failed to load final output:\n{str(e)}")
                return

        except Exception as e:
            self.logger.error(f"Error in Document Processing: {str(e)}", exc_info=True)
            messagebox.showerror("Error", f"Document Processing error:\n{str(e)}")
        finally:
            try:
                self.root.after(
                    0,
                    lambda: start_button.configure(state="normal", text="Run Document Processing")
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
        
        # PDF File Selection
        pdf_frame = ctk.CTkFrame(main_frame)
        pdf_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(pdf_frame, text="PDF File:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'ocr_extraction_pdf_var'):
            self.ocr_extraction_pdf_var = ctk.StringVar()
        
        entry_frame_pdf = ctk.CTkFrame(pdf_frame)
        entry_frame_pdf.pack(fill="x", padx=10, pady=5)
        
        pdf_entry = ctk.CTkEntry(entry_frame_pdf, textvariable=self.ocr_extraction_pdf_var)
        pdf_entry.pack(side="left", fill="x", expand=True, padx=5)
        
        ctk.CTkButton(entry_frame_pdf, text="Browse", 
                     command=lambda: self.browse_file_for_stage(self.ocr_extraction_pdf_var, 
                                                                 filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")])).pack(side="right")
        
        # Validation indicator
        if not hasattr(self, 'ocr_extraction_pdf_valid'):
            self.ocr_extraction_pdf_valid = ctk.CTkLabel(entry_frame_pdf, text="", width=30)
        self.ocr_extraction_pdf_valid.pack(side="right", padx=5)
        
        # Pre-OCR Topic File Selection (t{book}{chapter}.json)
        topic_frame = ctk.CTkFrame(main_frame)
        topic_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(topic_frame, text="Pre-OCR Topic File (t{book}{chapter}.json):", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'ocr_extraction_topic_var'):
            self.ocr_extraction_topic_var = ctk.StringVar()
        
        entry_frame_topic = ctk.CTkFrame(topic_frame)
        entry_frame_topic.pack(fill="x", padx=10, pady=5)
        
        topic_entry = ctk.CTkEntry(entry_frame_topic, textvariable=self.ocr_extraction_topic_var)
        topic_entry.pack(side="left", fill="x", expand=True, padx=5)
        
        ctk.CTkButton(entry_frame_topic, text="Browse", 
                     command=lambda: self.browse_file_for_stage(self.ocr_extraction_topic_var, 
                                                                 filetypes=[("JSON", "*.json")])).pack(side="right")
        
        # Validation indicator
        if not hasattr(self, 'ocr_extraction_topic_valid'):
            self.ocr_extraction_topic_valid = ctk.CTkLabel(entry_frame_topic, text="", width=30)
        self.ocr_extraction_topic_valid.pack(side="right", padx=5)
        
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
        
        if not hasattr(self, 'ocr_extraction_process_btn'):
            self.ocr_extraction_process_btn = ctk.CTkButton(
                process_frame,
                text="Extract OCR",
                command=self.process_ocr_extraction,
                width=200,
                height=40,
                font=ctk.CTkFont(size=14, weight="bold")
            )
        self.ocr_extraction_process_btn.pack(side="left", padx=10, pady=10)
        
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
        
        # Content Processing File Selection
        stage4_frame = ctk.CTkFrame(main_frame)
        stage4_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(stage4_frame, text="Content Processing JSON (with PointId):", 
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
        
        # OCR Extraction JSON File Selection
        ocr_extraction_frame = ctk.CTkFrame(main_frame)
        ocr_extraction_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(ocr_extraction_frame, text="OCR Extraction JSON File:", 
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_e_ocr_extraction_json_var'):
            self.stage_e_ocr_extraction_json_var = ctk.StringVar()
            # Auto-fill if available
            if hasattr(self, 'last_ocr_extraction_path') and self.last_ocr_extraction_path and os.path.exists(self.last_ocr_extraction_path):
                self.stage_e_ocr_extraction_json_var.set(self.last_ocr_extraction_path)
        
        entry_frame_ocr = ctk.CTkFrame(ocr_extraction_frame)
        entry_frame_ocr.pack(fill="x", padx=10, pady=5)
        
        ocr_extraction_entry = ctk.CTkEntry(entry_frame_ocr, textvariable=self.stage_e_ocr_extraction_json_var, width=400)
        ocr_extraction_entry.pack(side="left", fill="x", expand=True, padx=5)
        
        ctk.CTkButton(entry_frame_ocr, text="Browse", 
                     command=lambda: self.browse_file_for_stage(self.stage_e_ocr_extraction_json_var, 
                                                                 filetypes=[("JSON files", "*.json"), ("All files", "*.*")])).pack(side="right")
        
        # Validation indicator
        if not hasattr(self, 'stage_e_ocr_extraction_valid'):
            self.stage_e_ocr_extraction_valid = ctk.CTkLabel(entry_frame_ocr, text="", width=30)
        self.stage_e_ocr_extraction_valid.pack(side="right", padx=5)
        
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
        
        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)
        
        self.stage_e_process_btn = ctk.CTkButton(
            process_btn_frame,
            text="Process Image Notes Generation",
            command=self.process_stage_e,
            width=200,
            height=40,
            font=ctk.CTkFont(size=16, weight="bold"),
            fg_color="blue"
        )
        self.stage_e_process_btn.pack(pady=10)
        
        # View CSV button
        self.stage_e_view_csv_btn = ctk.CTkButton(
            process_btn_frame,
            text="View CSV",
            command=self.view_csv_stage_e,
            width=150,
            height=40,
            font=ctk.CTkFont(size=14),
            fg_color="green",
            hover_color="darkgreen",
            state="disabled"
        )
        self.stage_e_view_csv_btn.pack(pady=10)
        
        # Status for Stage E
        self.stage_e_status_label = ctk.CTkLabel(main_frame, text="Ready", 
                                                 font=ctk.CTkFont(size=12), text_color="gray")
        self.stage_e_status_label.pack(pady=10)
        
        # Auto-validate files on change
        self.stage_e_stage4_var.trace('w', lambda *args: self.validate_stage_e_files())
        if hasattr(self, 'stage_e_ocr_extraction_json_var'):
            self.stage_e_ocr_extraction_json_var.trace('w', lambda *args: self.validate_stage_e_files())
    
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
        ocr_extraction_path = self.stage_e_ocr_extraction_json_var.get() if hasattr(self, 'stage_e_ocr_extraction_json_var') else ""
        
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
        
        # Validate OCR Extraction JSON
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
                
                # Validate inputs
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
                self.root.after(0, lambda: self.stage_e_process_btn.configure(state="normal", text="Process Image Notes Generation"))
        
        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
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
                    self.ocr_extraction_status_label.configure(text="Error: Please select a valid Pre-OCR Topic file (t{book}{chapter}.json)", text_color="red")
                    messagebox.showerror("Error", "Please select a valid Pre-OCR Topic file (t{book}{chapter}.json)")
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
        
        # Image Notes File Selection
        stage_e_frame = ctk.CTkFrame(main_frame)
        stage_e_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(stage_e_frame, text="Image Notes JSON:", 
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
            text="Process Image File Catalog",
            command=self.process_stage_f,
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
                self.stage_f_status_label.configure(text="Processing Image File Catalog...", text_color="blue")
                
                # Validate inputs
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
        
        # Image File Catalog Selection (Optional)
        stage_f_frame = ctk.CTkFrame(main_frame)
        stage_f_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(stage_f_frame, text="Image File Catalog JSON (Optional - f.json):", 
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
                command=self.process_stage_j,
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
                self.stage_j_status_label.configure(text="Processing Importance & Type Tagging...", text_color="blue")
                
                # Validate inputs
                stage_e_path = self.stage_j_stage_e_var.get().strip()
                word_path = self.stage_j_word_var.get().strip()
                stage_f_path = self.stage_j_stage_f_var.get().strip() if hasattr(self, 'stage_j_stage_f_var') else ""
                prompt = self.stage_j_prompt_text.get("1.0", tk.END).strip()
                # Always use default model from main view settings
                model_name = self.get_default_model()
                
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
                    messagebox.showerror("Error", "Image File Catalog JSON file path is invalid")
                    return
                
                # Validate API keys
                if not self.api_key_manager.api_keys:
                    messagebox.showerror("Error", "Please load API keys first")
                    return
                
                def progress_callback(msg: str):
                    self.root.after(0, lambda: self.stage_j_status_label.configure(text=msg))
                
                # Process Stage J (use default output dir from main view)
                output_path = self.stage_j_processor.process_stage_j(
                    stage_e_path=stage_e_path,
                    word_file_path=word_path,
                    stage_f_path=stage_f_path if stage_f_path else None,
                    prompt=prompt,
                    model_name=model_name,
                    output_dir=self.get_default_output_dir(stage_e_path),
                    progress_callback=progress_callback
                )
                
                if output_path:
                    self.last_stage_j_path = output_path
                    self.update_stage_status("J", "completed", output_path)
                    self.root.after(0, lambda: self.stage_j_view_csv_btn.configure(state="normal"))
                    self.stage_j_status_label.configure(
                        text=f"Stage J completed successfully!\nOutput: {os.path.basename(output_path)}",
                        text_color="green"
                    )
                    messagebox.showinfo("Success", f"Importance & Type Tagging completed!\n\nOutput saved to:\n{output_path}")
                else:
                    self.update_stage_status("J", "error")
                    self.stage_j_status_label.configure(text="Importance & Type Tagging failed. Check logs for details.", text_color="red")
                    messagebox.showerror("Error", "Importance & Type Tagging processing failed. Check logs for details.")
            
            except Exception as e:
                self.logger.error(f"Error in Importance & Type Tagging processing: {e}", exc_info=True)
                self.update_stage_status("J", "error")
                self.stage_j_status_label.configure(text=f"Error: {str(e)}", text_color="red")
                messagebox.showerror("Error", f"Importance & Type Tagging processing error:\n{str(e)}")
            finally:
                self.root.after(0, lambda: self.stage_j_process_btn.configure(state="normal", text="Process Importance & Type Tagging"))
        
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
                self.stage_h_status_label.configure(text="Processing Flashcard Generation...", text_color="blue")
                
                # Validate inputs
                stage_j_path = self.stage_h_stage_j_var.get().strip()
                stage_f_path = self.stage_h_stage_f_var.get().strip()
                prompt = self.stage_h_prompt_text.get("1.0", tk.END).strip()
                # Always use default model from main view settings
                model_name = self.get_default_model()
                
                if not stage_j_path or not os.path.exists(stage_j_path):
                    messagebox.showerror("Error", "Please select a valid Tagged Data JSON file")
                    return
                
                if not stage_f_path or not os.path.exists(stage_f_path):
                    messagebox.showerror("Error", "Please select a valid Image File Catalog JSON file")
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
                
                # Process Stage H (always use default output dir from main view)
                output_path = self.stage_h_processor.process_stage_h(
                    stage_j_path=stage_j_path,
                    stage_f_path=stage_f_path,
                    prompt=prompt,
                    model_name=model_name,
                    output_dir=self.get_default_output_dir(stage_j_path),
                    progress_callback=progress_callback
                )
                
                if output_path:
                    self.last_stage_h_path = output_path
                    self.update_stage_status("H", "completed", output_path)
                    self.root.after(0, lambda: self.stage_h_view_csv_btn.configure(state="normal"))
                    self.stage_h_status_label.configure(
                        text=f"Stage H completed successfully!\nOutput: {os.path.basename(output_path)}",
                        text_color="green"
                    )
                    messagebox.showinfo("Success", f"Flashcard Generation completed!\n\nOutput saved to:\n{output_path}")
                else:
                    self.update_stage_status("H", "error")
                    self.stage_h_status_label.configure(text="Flashcard Generation failed. Check logs for details.", text_color="red")
                    messagebox.showerror("Error", "Flashcard Generation processing failed. Check logs for details.")
            
            except Exception as e:
                self.logger.error(f"Error in Flashcard Generation processing: {e}", exc_info=True)
                self.update_stage_status("H", "error")
                self.stage_h_status_label.configure(text=f"Error: {str(e)}", text_color="red")
                messagebox.showerror("Error", f"Flashcard Generation processing error:\n{str(e)}")
            finally:
                self.root.after(0, lambda: self.stage_h_process_btn.configure(state="normal", text="Process Flashcard Generation"))
        
        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
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
        
        desc = ctk.CTkLabel(main_frame, text="Generate flashcards from Tagged data and Image File Catalog.", 
                           font=ctk.CTkFont(size=12), text_color="gray")
        desc.pack(pady=(0, 20))
        
        # Tagged Data File Selection
        stage_j_frame = ctk.CTkFrame(main_frame)
        stage_j_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(stage_j_frame, text="Tagged Data JSON (a file):", 
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
        
        # Image File Catalog Selection
        stage_f_frame = ctk.CTkFrame(main_frame)
        stage_f_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(stage_f_frame, text="Image File Catalog JSON (f.json):", 
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
        
        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)
        
        if not hasattr(self, 'stage_h_process_btn'):
            self.stage_h_process_btn = ctk.CTkButton(
                process_btn_frame,
                text="Process Flashcard Generation",
                command=self.process_stage_h,
                font=ctk.CTkFont(size=16, weight="bold"),
                height=40
            )
        self.stage_h_process_btn.pack(pady=10)
        
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
        
        # Bind validation
        self.stage_h_stage_j_var.trace('w', lambda *args: self.validate_stage_h_files())
        self.stage_h_stage_f_var.trace('w', lambda *args: self.validate_stage_h_files())
        
        # Initial validation
        self.validate_stage_h_files()
    
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
        
        desc = ctk.CTkLabel(main_frame, text="Generate test files from Tagged data and Word document in three steps.", 
                           font=ctk.CTkFont(size=12), text_color="gray")
        desc.pack(pady=(0, 20))
        
        # Tagged Data File Selection
        stage_j_frame = ctk.CTkFrame(main_frame)
        stage_j_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(stage_j_frame, text="Tagged Data JSON (a file):", 
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
        
        # OCR Extraction JSON File Selection
        ocr_extraction_frame = ctk.CTkFrame(main_frame)
        ocr_extraction_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(ocr_extraction_frame, text="OCR Extraction JSON File:", 
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
        
        # Validation indicator
        if not hasattr(self, 'stage_v_ocr_extraction_valid'):
            self.stage_v_ocr_extraction_valid = ctk.CTkLabel(entry_frame_ocr, text="", width=30)
        self.stage_v_ocr_extraction_valid.pack(side="right", padx=5)
        
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
        
        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)
        
        if not hasattr(self, 'stage_v_process_btn'):
            self.stage_v_process_btn = ctk.CTkButton(
                process_btn_frame,
                text="Process Test Bank Generation",
                command=self.process_stage_v,
                font=ctk.CTkFont(size=16, weight="bold"),
                height=40
            )
        self.stage_v_process_btn.pack(pady=10)
        
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
        
        # Bind validation
        self.stage_v_stage_j_var.trace('w', lambda *args: self.validate_stage_v_files())
        self.stage_v_word_var.trace('w', lambda *args: self.validate_stage_v_files())
        if hasattr(self, 'stage_v_ocr_extraction_var'):
            self.stage_v_ocr_extraction_var.trace('w', lambda *args: self.validate_stage_v_files())
        
        # Initial validation
        self.validate_stage_v_files()
    
    def validate_stage_v_files(self):
        """Validate Stage V input files"""
        stage_j_path = self.stage_v_stage_j_var.get().strip()
        word_path = self.stage_v_word_var.get().strip()
        ocr_extraction_path = self.stage_v_ocr_extraction_var.get().strip() if hasattr(self, 'stage_v_ocr_extraction_var') else ""
        
        # Validate Stage J file
        if stage_j_path and os.path.exists(stage_j_path):
            self.stage_v_stage_j_valid.configure(text="✓", text_color="green")
        else:
            self.stage_v_stage_j_valid.configure(text="", text_color="red")
        
        # Validate Word file
        if word_path and os.path.exists(word_path):
            self.stage_v_word_valid.configure(text="✓", text_color="green")
        else:
            self.stage_v_word_valid.configure(text="", text_color="red")
        
        # Validate OCR Extraction file
        if hasattr(self, 'stage_v_ocr_extraction_valid'):
            if ocr_extraction_path and os.path.exists(ocr_extraction_path):
                self.stage_v_ocr_extraction_valid.configure(text="✓", text_color="green")
            else:
                self.stage_v_ocr_extraction_valid.configure(text="", text_color="red")
    
    def process_stage_v(self):
        """Process Stage V in background thread"""
        def worker():
            try:
                self.stage_v_process_btn.configure(state="disabled", text="Processing...")
                self.update_stage_status("V", "processing")
                self.stage_v_status_label.configure(text="Processing Test Bank Generation...", text_color="blue")
                
                # Validate inputs
                stage_j_path = self.stage_v_stage_j_var.get().strip()
                word_path = self.stage_v_word_var.get().strip()
                ocr_extraction_path = self.stage_v_ocr_extraction_var.get().strip() if hasattr(self, 'stage_v_ocr_extraction_var') else ""
                prompt_1 = self.stage_v_prompt1_text.get("1.0", tk.END).strip()
                # Use model from combo box if available, otherwise use default
                model_name_1 = self.stage_v_model1_var.get() if hasattr(self, 'stage_v_model1_var') else self.get_default_model()
                prompt_2 = self.stage_v_prompt2_text.get("1.0", tk.END).strip()
                model_name_2 = self.stage_v_model2_var.get() if hasattr(self, 'stage_v_model2_var') else self.get_default_model()
                
                if not stage_j_path or not os.path.exists(stage_j_path):
                    messagebox.showerror("Error", "Please select a valid Tagged Data JSON file")
                    return
                
                if not word_path or not os.path.exists(word_path):
                    messagebox.showerror("Error", "Please select a valid Word document")
                    return
                
                if not ocr_extraction_path or not os.path.exists(ocr_extraction_path):
                    messagebox.showerror("Error", "Please select a valid OCR Extraction JSON file")
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
                
                # Process Stage V (use default output dir from main view)
                output_path = self.stage_v_processor.process_stage_v(
                    stage_j_path=stage_j_path,
                    word_file_path=word_path,
                    ocr_extraction_json_path=ocr_extraction_path,
                    prompt_1=prompt_1,
                    model_name_1=model_name_1,
                    prompt_2=prompt_2,
                    model_name_2=model_name_2,
                    output_dir=self.get_default_output_dir(stage_j_path),
                    progress_callback=progress_callback
                )
                
                if output_path:
                    self.last_stage_v_path = output_path
                    self.update_stage_status("V", "completed", output_path)
                    self.root.after(0, lambda: self.stage_v_view_csv_btn.configure(state="normal"))
                    self.stage_v_status_label.configure(
                        text=f"Stage V completed successfully!\nOutput: {os.path.basename(output_path)}",
                        text_color="green"
                    )
                    messagebox.showinfo("Success", f"Test Bank Generation completed!\n\nOutput saved to:\n{output_path}")
                else:
                    self.update_stage_status("V", "error")
                    self.stage_v_status_label.configure(text="Test Bank Generation failed. Check logs for details.", text_color="red")
                    messagebox.showerror("Error", "Test Bank Generation processing failed. Check logs for details.")
            
            except Exception as e:
                self.logger.error(f"Error in Test Bank Generation processing: {e}", exc_info=True)
                self.update_stage_status("V", "error")
                self.stage_v_status_label.configure(text=f"Error: {str(e)}", text_color="red")
                messagebox.showerror("Error", f"Test Bank Generation processing error:\n{str(e)}")
            finally:
                self.root.after(0, lambda: self.stage_v_process_btn.configure(state="normal", text="Process Test Bank Generation"))
        
        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def setup_stage_m_ui(self, parent):
        """Setup UI for Stage M: Topic ID List"""
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
        
        desc = ctk.CTkLabel(main_frame, text="Extract unique chapter / subchapter / topic combinations from Flashcard file (ac).", 
                           font=ctk.CTkFont(size=12), text_color="gray")
        desc.pack(pady=(0, 20))

        # Flashcard File Selection
        stage_h_frame = ctk.CTkFrame(main_frame)
        stage_h_frame.pack(fill="x", pady=10)

        ctk.CTkLabel(stage_h_frame, text="Flashcard JSON (ac file):",
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
                text="Process Topic List Extraction",
                command=self.process_stage_m,
                font=ctk.CTkFont(size=16, weight="bold"),
                height=40
            )
        self.stage_m_process_btn.pack(pady=10)

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
                self.stage_m_status_label.configure(text="Processing Topic List Extraction...", text_color="blue")

                # Validate inputs
                stage_h_path = self.stage_m_stage_h_var.get().strip()

                if not stage_h_path or not os.path.exists(stage_h_path):
                    messagebox.showerror("Error", "Please select a valid Flashcard JSON file")
                    return

                # Validate API keys (not strictly needed for Stage M, but keep consistent)
                if not self.api_key_manager.api_keys:
                    messagebox.showerror("Error", "Please load API keys first")
                    return

                def progress_callback(msg: str):
                    self.root.after(0, lambda: self.stage_m_status_label.configure(text=msg))

                # Detect book/chapter for status convenience (optional)
                # Always use default output dir from main view
                output_path = self.stage_m_processor.process_stage_m(
                    stage_h_path=stage_h_path,
                    output_dir=self.get_default_output_dir(stage_h_path),
                    progress_callback=progress_callback
                )

                if output_path:
                    self.last_stage_m_path = output_path
                    self.update_stage_status("M", "completed", output_path)
                    self.root.after(0, lambda: self.stage_m_view_csv_btn.configure(state="normal"))
                    self.stage_m_status_label.configure(
                        text=f"Stage M completed successfully!\nOutput: {os.path.basename(output_path)}",
                        text_color="green"
                    )
                    messagebox.showinfo("Success", f"Topic List Extraction completed!\n\nOutput saved to:\n{output_path}")
                else:
                    self.update_stage_status("M", "error")
                    self.stage_m_status_label.configure(text="Topic List Extraction failed. Check logs for details.", text_color="red")
                    messagebox.showerror("Error", "Topic List Extraction processing failed. Check logs for details.")

            except Exception as e:
                self.logger.error(f"Error in Topic List Extraction processing: {e}", exc_info=True)
                self.update_stage_status("M", "error")
                self.stage_m_status_label.configure(text=f"Error: {str(e)}", text_color="red")
                messagebox.showerror("Error", f"Topic List Extraction processing error:\n{str(e)}")
            finally:
                self.root.after(0, lambda: self.stage_m_process_btn.configure(state="normal", text="Process Topic List Extraction"))

        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def setup_stage_l_ui(self, parent):
        """Setup UI for Stage L: Chapter Overview"""
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
            text="Generate per-topic chapter overview from Tagged data (a file) and Test Bank (b file).",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        desc.pack(pady=(0, 20))

        # Tagged Data File Selection
        stage_j_frame = ctk.CTkFrame(main_frame)
        stage_j_frame.pack(fill="x", pady=10)

        ctk.CTkLabel(stage_j_frame, text="Tagged Data JSON (a file):",
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

        ctk.CTkLabel(stage_v_frame, text="Test Bank JSON (b file):",
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

        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)

        if not hasattr(self, 'stage_l_process_btn'):
            self.stage_l_process_btn = ctk.CTkButton(
                process_btn_frame,
                text="Process Chapter Summary",
                command=self.process_stage_l,
                font=ctk.CTkFont(size=16, weight="bold"),
                height=40
            )
        self.stage_l_process_btn.pack(pady=10)

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
                self.stage_l_status_label.configure(text="Processing Chapter Summary...", text_color="blue")

                # Validate inputs
                stage_j_path = self.stage_l_stage_j_var.get().strip()
                stage_v_path = self.stage_l_stage_v_var.get().strip()

                if not stage_j_path or not os.path.exists(stage_j_path):
                    messagebox.showerror("Error", "Please select a valid Tagged Data JSON file (a file)")
                    return

                if not stage_v_path or not os.path.exists(stage_v_path):
                    messagebox.showerror("Error", "Please select a valid Test Bank JSON file (b file)")
                    return

                prompt = self.stage_l_prompt_text.get("1.0", tk.END).strip()
                # Always use default model from main view settings
                model_name = self.get_default_model()

                if not prompt:
                    messagebox.showerror("Error", "Please enter a prompt for Chapter Summary")
                    return

                # Validate API keys (to be consistent with other stages)
                if not self.api_key_manager.api_keys:
                    messagebox.showerror("Error", "Please load API keys first")
                    return

                def progress_callback(msg: str):
                    self.root.after(0, lambda: self.stage_l_status_label.configure(text=msg))

                # Process Stage L (always use default output dir from main view)
                output_path = self.stage_l_processor.process_stage_l(
                    stage_j_path=stage_j_path,
                    stage_v_path=stage_v_path,
                    prompt=prompt,
                    model_name=model_name,
                    output_dir=self.get_default_output_dir(stage_j_path),
                    progress_callback=progress_callback
                )

                if output_path:
                    self.last_stage_l_path = output_path
                    self.update_stage_status("L", "completed", output_path)
                    self.root.after(0, lambda: self.stage_l_view_csv_btn.configure(state="normal"))
                    self.stage_l_status_label.configure(
                        text=f"Stage L completed successfully!\nOutput: {os.path.basename(output_path)}",
                        text_color="green"
                    )
                    messagebox.showinfo("Success", f"Chapter Summary completed!\n\nOutput saved to:\n{output_path}")
                else:
                    self.update_stage_status("L", "error")
                    self.stage_l_status_label.configure(text="Chapter Summary failed. Check logs for details.", text_color="red")
                    messagebox.showerror("Error", "Chapter Summary processing failed. Check logs for details.")

            except Exception as e:
                self.logger.error(f"Error in Chapter Summary processing: {e}", exc_info=True)
                self.update_stage_status("L", "error")
                self.stage_l_status_label.configure(text=f"Error: {str(e)}", text_color="red")
                messagebox.showerror("Error", f"Chapter Summary processing error:\n{str(e)}")
            finally:
                self.root.after(0, lambda: self.stage_l_process_btn.configure(state="normal", text="Process Chapter Summary"))

        # Run in background thread
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
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
            text="Detect changes between old book PDF and current Stage A data. Part 1: Extract PDF. Part 2: Detect changes.",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        desc.pack(pady=(0, 20))
        
        # Part 1: PDF Extraction
        part1_frame = ctk.CTkFrame(main_frame)
        part1_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(part1_frame, text="Part 1: PDF Extraction",
                    font=ctk.CTkFont(size=16, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        # Old Book PDF
        pdf_frame = ctk.CTkFrame(part1_frame)
        pdf_frame.pack(fill="x", padx=10, pady=5)
        ctk.CTkLabel(pdf_frame, text="Old Book PDF:", width=150).pack(side="left", padx=5)
        if not hasattr(self, 'stage_x_old_pdf_var'):
            self.stage_x_old_pdf_var = ctk.StringVar()
        ctk.CTkEntry(pdf_frame, textvariable=self.stage_x_old_pdf_var, width=400).pack(side="left", padx=5, fill="x", expand=True)
        ctk.CTkButton(
            pdf_frame,
            text="Browse",
            command=lambda: self.stage_x_old_pdf_var.set(filedialog.askopenfilename(filetypes=[("PDF files", "*.pdf")]))
        ).pack(side="left", padx=5)
        
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
        
        # Part 2: Change Detection
        part2_frame = ctk.CTkFrame(main_frame)
        part2_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(part2_frame, text="Part 2: Change Detection",
                    font=ctk.CTkFont(size=16, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        # Stage A File
        stage_a_frame = ctk.CTkFrame(part2_frame)
        stage_a_frame.pack(fill="x", padx=10, pady=5)
        ctk.CTkLabel(stage_a_frame, text="Stage A JSON (a file):", width=150).pack(side="left", padx=5)
        if not hasattr(self, 'stage_x_stage_a_var'):
            self.stage_x_stage_a_var = ctk.StringVar()
            if hasattr(self, 'last_stage_j_path') and self.last_stage_j_path:
                self.stage_x_stage_a_var.set(self.last_stage_j_path)
        ctk.CTkEntry(stage_a_frame, textvariable=self.stage_x_stage_a_var, width=400).pack(side="left", padx=5, fill="x", expand=True)
        ctk.CTkButton(
            stage_a_frame,
            text="Browse",
            command=lambda: self.stage_x_stage_a_var.set(filedialog.askopenfilename(filetypes=[("JSON files", "*.json")]))
        ).pack(side="left", padx=5)
        
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
        
        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)
        
        if not hasattr(self, 'stage_x_process_btn'):
            self.stage_x_process_btn = ctk.CTkButton(
                process_btn_frame,
                text="Process Book Changes Detection",
                command=self.process_stage_x,
                font=ctk.CTkFont(size=16, weight="bold"),
                height=40
            )
        self.stage_x_process_btn.pack(pady=10)
        
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
            text="Step 1: Extract PDF from old reference (2 parts). Step 2: Detect deletions by comparing OCR Extraction JSON with extracted PDF.",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        desc.pack(pady=(0, 20))
        
        # Step 1: Old Reference PDF File
        old_pdf_frame = ctk.CTkFrame(main_frame)
        old_pdf_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(old_pdf_frame, text="Old Reference PDF File (Step 1):",
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_y_old_pdf_var'):
            self.stage_y_old_pdf_var = ctk.StringVar()
        
        entry_frame_old_pdf = ctk.CTkFrame(old_pdf_frame)
        entry_frame_old_pdf.pack(fill="x", padx=10, pady=5)
        ctk.CTkEntry(entry_frame_old_pdf, textvariable=self.stage_y_old_pdf_var).pack(side="left", fill="x", expand=True, padx=5)
        ctk.CTkButton(
            entry_frame_old_pdf,
            text="Browse",
            command=lambda: self.stage_y_old_pdf_var.set(filedialog.askopenfilename(filetypes=[("PDF", "*.pdf")]))
        ).pack(side="right")
        
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
        
        # Step 2: OCR Extraction JSON File
        ocr_extraction_frame = ctk.CTkFrame(main_frame)
        ocr_extraction_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(ocr_extraction_frame, text="OCR Extraction JSON (Step 2 - from OCR Extraction stage):",
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_y_ocr_extraction_var'):
            self.stage_y_ocr_extraction_var = ctk.StringVar()
            # Try to auto-fill from last OCR extraction output if available
            if hasattr(self, 'ocr_extraction_output_var') and self.ocr_extraction_output_var.get():
                self.stage_y_ocr_extraction_var.set(self.ocr_extraction_output_var.get())
        
        entry_frame_ocr = ctk.CTkFrame(ocr_extraction_frame)
        entry_frame_ocr.pack(fill="x", padx=10, pady=5)
        ctk.CTkEntry(entry_frame_ocr, textvariable=self.stage_y_ocr_extraction_var).pack(side="left", fill="x", expand=True, padx=5)
        ctk.CTkButton(
            entry_frame_ocr,
            text="Browse",
            command=lambda: self.stage_y_ocr_extraction_var.set(filedialog.askopenfilename(filetypes=[("JSON", "*.json")]))
        ).pack(side="right")
        
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
        
        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)
        
        if not hasattr(self, 'stage_y_process_btn'):
            self.stage_y_process_btn = ctk.CTkButton(
                process_btn_frame,
                text="Process Deletion Detection",
                command=self.process_stage_y,
                font=ctk.CTkFont(size=16, weight="bold"),
                height=40
            )
        self.stage_y_process_btn.pack(pady=10)
        
        # Status Label
        if not hasattr(self, 'stage_y_status_label'):
            self.stage_y_status_label = ctk.CTkLabel(
                process_btn_frame,
                text="Ready",
                font=ctk.CTkFont(size=12),
                text_color="gray"
            )
        self.stage_y_status_label.pack(pady=5)
    
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
            text="Generate RichText format output from Stage A, Stage X, and Stage Y data.",
            font=ctk.CTkFont(size=12),
            text_color="gray"
        )
        desc.pack(pady=(0, 20))
        
        # Stage A File
        stage_a_frame = ctk.CTkFrame(main_frame)
        stage_a_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(stage_a_frame, text="Stage A JSON (a file):",
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_z_stage_a_var'):
            self.stage_z_stage_a_var = ctk.StringVar()
            if hasattr(self, 'last_stage_j_path') and self.last_stage_j_path:
                self.stage_z_stage_a_var.set(self.last_stage_j_path)
        
        entry_frame_a = ctk.CTkFrame(stage_a_frame)
        entry_frame_a.pack(fill="x", padx=10, pady=5)
        ctk.CTkEntry(entry_frame_a, textvariable=self.stage_z_stage_a_var).pack(side="left", fill="x", expand=True, padx=5)
        ctk.CTkButton(
            entry_frame_a,
            text="Browse",
            command=lambda: self.stage_z_stage_a_var.set(filedialog.askopenfilename(filetypes=[("JSON", "*.json")]))
        ).pack(side="right")
        
        # Stage X Output
        stage_x_frame = ctk.CTkFrame(main_frame)
        stage_x_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(stage_x_frame, text="Stage X Output JSON:",
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_z_stage_x_var'):
            self.stage_z_stage_x_var = ctk.StringVar()
        
        entry_frame_x = ctk.CTkFrame(stage_x_frame)
        entry_frame_x.pack(fill="x", padx=10, pady=5)
        ctk.CTkEntry(entry_frame_x, textvariable=self.stage_z_stage_x_var).pack(side="left", fill="x", expand=True, padx=5)
        ctk.CTkButton(
            entry_frame_x,
            text="Browse",
            command=lambda: self.stage_z_stage_x_var.set(filedialog.askopenfilename(filetypes=[("JSON", "*.json")]))
        ).pack(side="right")
        
        # Stage Y Output
        stage_y_frame = ctk.CTkFrame(main_frame)
        stage_y_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(stage_y_frame, text="Stage Y Output JSON:",
                    font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=5)
        
        if not hasattr(self, 'stage_z_stage_y_var'):
            self.stage_z_stage_y_var = ctk.StringVar()
        
        entry_frame_y = ctk.CTkFrame(stage_y_frame)
        entry_frame_y.pack(fill="x", padx=10, pady=5)
        ctk.CTkEntry(entry_frame_y, textvariable=self.stage_z_stage_y_var).pack(side="left", fill="x", expand=True, padx=5)
        ctk.CTkButton(
            entry_frame_y,
            text="Browse",
            command=lambda: self.stage_z_stage_y_var.set(filedialog.askopenfilename(filetypes=[("JSON", "*.json")]))
        ).pack(side="right")
        
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
        
        # Process Button
        process_btn_frame = ctk.CTkFrame(main_frame)
        process_btn_frame.pack(fill="x", pady=20)
        
        if not hasattr(self, 'stage_z_process_btn'):
            self.stage_z_process_btn = ctk.CTkButton(
                process_btn_frame,
                text="Process RichText Generation",
                command=self.process_stage_z,
                font=ctk.CTkFont(size=16, weight="bold"),
                height=40
            )
        self.stage_z_process_btn.pack(pady=10)
        
        # Status Label
        if not hasattr(self, 'stage_z_status_label'):
            self.stage_z_status_label = ctk.CTkLabel(
                process_btn_frame,
                text="Ready",
                font=ctk.CTkFont(size=12),
                text_color="gray"
            )
        self.stage_z_status_label.pack(pady=5)
    
    def process_stage_x(self):
        """Process Stage X in background thread"""
        def worker():
            try:
                self.stage_x_process_btn.configure(state="disabled", text="Processing...")
                self.stage_x_status_label.configure(text="Processing Book Changes Detection...", text_color="blue")
                
                # Validate inputs
                old_pdf_path = self.stage_x_old_pdf_var.get().strip()
                stage_a_path = self.stage_x_stage_a_var.get().strip()
                pdf_prompt = self.stage_x_pdf_prompt_text.get("1.0", tk.END).strip()
                change_prompt = self.stage_x_change_prompt_text.get("1.0", tk.END).strip()
                
                if not old_pdf_path or not os.path.exists(old_pdf_path):
                    messagebox.showerror("Error", "Please select a valid old book PDF file.")
                    return
                
                if not stage_a_path or not os.path.exists(stage_a_path):
                    messagebox.showerror("Error", "Please select a valid Stage A JSON file.")
                    return
                
                if not pdf_prompt:
                    messagebox.showerror("Error", "Please enter PDF extraction prompt.")
                    return
                
                if not change_prompt:
                    messagebox.showerror("Error", "Please enter change detection prompt.")
                    return
                
                # Always use default model from main view settings
                pdf_model = self.get_default_model()
                change_model = self.get_default_model()
                
                def progress_callback(msg: str):
                    self.root.after(0, lambda: self.stage_x_status_label.configure(text=msg))
                
                output_path = self.stage_x_processor.process_stage_x(
                    old_book_pdf_path=old_pdf_path,
                    pdf_extraction_prompt=pdf_prompt,
                    pdf_extraction_model=pdf_model,
                    stage_a_path=stage_a_path,
                    changes_prompt=change_prompt,
                    changes_model=change_model,
                    output_dir=self.get_default_output_dir(stage_a_path),
                    progress_callback=progress_callback
                )
                
                if output_path:
                    self.last_stage_x_path = output_path
                    self.stage_x_status_label.configure(
                        text=f"Stage X completed successfully!\nOutput: {os.path.basename(output_path)}",
                        text_color="green"
                    )
                    messagebox.showinfo("Success", f"Book Changes Detection completed!\n\nOutput saved to:\n{output_path}")
                else:
                    self.stage_x_status_label.configure(text="Book Changes Detection failed. Check logs for details.", text_color="red")
                    messagebox.showerror("Error", "Book Changes Detection failed. Check logs for details.")
                    
            except Exception as e:
                self.logger.error(f"Error in Stage X processing: {e}", exc_info=True)
                self.stage_x_status_label.configure(text=f"Error: {str(e)}", text_color="red")
                messagebox.showerror("Error", f"Book Changes Detection error:\n{str(e)}")
            finally:
                self.root.after(0, lambda: self.stage_x_process_btn.configure(state="normal", text="Process Book Changes Detection"))
        
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def process_stage_y(self):
        """Process Stage Y in background thread"""
        def worker():
            try:
                self.stage_y_process_btn.configure(state="disabled", text="Processing...")
                self.stage_y_status_label.configure(text="Processing Deletion Detection...", text_color="blue")
                
                # Validate inputs
                old_pdf_path = self.stage_y_old_pdf_var.get().strip()
                ocr_extraction_prompt = self.stage_y_ocr_prompt_text.get("1.0", tk.END).strip()
                # Always use default model from main view settings
                ocr_extraction_model = self.get_default_model()
                ocr_extraction_json_path = self.stage_y_ocr_extraction_var.get().strip()
                deletion_detection_prompt = self.stage_y_deletion_prompt_text.get("1.0", tk.END).strip()
                deletion_detection_model = self.get_default_model()
                
                if not old_pdf_path or not os.path.exists(old_pdf_path):
                    messagebox.showerror("Error", "Please select a valid old reference PDF file.")
                    return
                
                if not ocr_extraction_prompt:
                    messagebox.showerror("Error", "Please enter OCR Extraction prompt.")
                    return
                
                if not ocr_extraction_json_path or not os.path.exists(ocr_extraction_json_path):
                    messagebox.showerror("Error", "Please select a valid OCR Extraction JSON file.")
                    return
                
                if not deletion_detection_prompt:
                    messagebox.showerror("Error", "Please enter Deletion Detection prompt.")
                    return
                
                def progress_callback(msg: str):
                    self.root.after(0, lambda: self.stage_y_status_label.configure(text=msg))
                
                output_path = self.stage_y_processor.process_stage_y(
                    old_book_pdf_path=old_pdf_path,
                    ocr_extraction_prompt=ocr_extraction_prompt,
                    ocr_extraction_model=ocr_extraction_model,
                    ocr_extraction_json_path=ocr_extraction_json_path,
                    deletion_detection_prompt=deletion_detection_prompt,
                    deletion_detection_model=deletion_detection_model,
                    output_dir=self.get_default_output_dir(ocr_extraction_json_path),
                    progress_callback=progress_callback
                )
                
                if output_path:
                    self.last_stage_y_path = output_path
                    self.stage_y_status_label.configure(
                        text=f"Stage Y completed successfully!\nOutput: {os.path.basename(output_path)}",
                        text_color="green"
                    )
                    messagebox.showinfo("Success", f"Deletion Detection completed!\n\nOutput saved to:\n{output_path}")
                else:
                    self.stage_y_status_label.configure(text="Deletion Detection failed. Check logs for details.", text_color="red")
                    messagebox.showerror("Error", "Deletion Detection failed. Check logs for details.")
                    
            except Exception as e:
                self.logger.error(f"Error in Stage Y processing: {e}", exc_info=True)
                self.stage_y_status_label.configure(text=f"Error: {str(e)}", text_color="red")
                messagebox.showerror("Error", f"Deletion Detection error:\n{str(e)}")
            finally:
                self.root.after(0, lambda: self.stage_y_process_btn.configure(state="normal", text="Process Deletion Detection"))
        
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
    
    def process_stage_z(self):
        """Process Stage Z in background thread"""
        def worker():
            try:
                self.stage_z_process_btn.configure(state="disabled", text="Processing...")
                self.stage_z_status_label.configure(text="Processing RichText Generation...", text_color="blue")
                
                # Validate inputs
                stage_a_path = self.stage_z_stage_a_var.get().strip()
                stage_x_path = self.stage_z_stage_x_var.get().strip()
                stage_y_path = self.stage_z_stage_y_var.get().strip()
                prompt = self.stage_z_prompt_text.get("1.0", tk.END).strip()
                
                if not stage_a_path or not os.path.exists(stage_a_path):
                    messagebox.showerror("Error", "Please select a valid Stage A JSON file.")
                    return
                
                if not stage_x_path or not os.path.exists(stage_x_path):
                    messagebox.showerror("Error", "Please select a valid Stage X output JSON file.")
                    return
                
                if not stage_y_path or not os.path.exists(stage_y_path):
                    messagebox.showerror("Error", "Please select a valid Stage Y output JSON file.")
                    return
                
                if not prompt:
                    messagebox.showerror("Error", "Please enter a prompt for RichText Generation.")
                    return
                
                # Always use default model from main view settings
                model_name = self.get_default_model()
                
                def progress_callback(msg: str):
                    self.root.after(0, lambda: self.stage_z_status_label.configure(text=msg))
                
                output_path = self.stage_z_processor.process_stage_z(
                    stage_a_path=stage_a_path,
                    stage_x_output_path=stage_x_path,
                    stage_y_output_path=stage_y_path,
                    prompt=prompt,
                    model_name=model_name,
                    output_dir=self.get_default_output_dir(stage_a_path),
                    progress_callback=progress_callback
                )
                
                if output_path:
                    self.last_stage_z_path = output_path
                    self.stage_z_status_label.configure(
                        text=f"Stage Z completed successfully!\nOutput: {os.path.basename(output_path)}",
                        text_color="green"
                    )
                    messagebox.showinfo("Success", f"RichText Generation completed!\n\nOutput saved to:\n{output_path}")
                else:
                    self.stage_z_status_label.configure(text="RichText Generation failed. Check logs for details.", text_color="red")
                    messagebox.showerror("Error", "RichText Generation failed. Check logs for details.")
                    
            except Exception as e:
                self.logger.error(f"Error in Stage Z processing: {e}", exc_info=True)
                self.stage_z_status_label.configure(text=f"Error: {str(e)}", text_color="red")
                messagebox.showerror("Error", f"RichText Generation error:\n{str(e)}")
            finally:
                self.root.after(0, lambda: self.stage_z_process_btn.configure(state="normal", text="Process RichText Generation"))
        
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
