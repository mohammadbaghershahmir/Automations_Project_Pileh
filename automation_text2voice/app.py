import tkinter as tk
from tkinter import ttk, messagebox, filedialog, scrolledtext
import customtkinter as ctk
import google.generativeai as genai
from google.cloud import texttospeech
import pygame
import os
import json
import threading
import base64
import wave
import asyncio
from typing import Optional, Dict, Any

class TextToSpeechApp:
    def __init__(self):
        # Configure appearance
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        
        # Initialize main window
        self.root = ctk.CTk()
        self.root.title("Advanced Text-to-Speech with Gemini AI")
        self.root.geometry("1200x900")
        self.root.minsize(1000, 700)
        
        # Initialize pygame mixer for audio playback
        pygame.mixer.init()
        
        # Configuration
        self.config = self.load_config()
        self.gemini_client = None
        self.gemini_tts_client = None
        self.tts_client = None
        
        # Available Gemini models
        self.gemini_models = [
            "gemini-2.5-flash",
            "gemini-2.5-pro", 
            "gemini-2.0-flash",
            "gemini-1.5-pro",
            "gemini-1.5-flash"
        ]
        
        # Available Gemini TTS models
        self.gemini_tts_models = [
            "gemini-2.5-flash-preview-tts",
            "gemini-2.5-pro-preview-tts"
        ]
        
        # Available Gemini TTS voices
        self.gemini_tts_voices = [
            "Kore", "Orus", "Autonoe", "Umbriel", "Erinome", "Laomedeia", 
            "Schedar", "Achird", "Sadachbia", "Zephyr", "Puck", "Charon",
            "Fenrir", "Aoede", "Enceladus", "Algieba", "Algenib", "Achernar",
            "Gacrux", "Zubenelgenubi", "Sadaltager", "Leda", "Callirrhoe",
            "Iapetus", "Despina", "Rasalgethi", "Alnilam", "Pulcherrima",
            "Vindemiatrix", "Sulafat"
        ]
        
        # TTS Service options
        self.tts_services = [
            "Gemini Native TTS",
            "Google Cloud TTS"
        ]
        
        # Google Cloud TTS voice options
        self.voice_options = {
            "en-US-Standard-A": "English (US) - Female",
            "en-US-Standard-B": "English (US) - Male",
            "en-US-Standard-C": "English (US) - Female",
            "en-US-Standard-D": "English (US) - Male",
            "en-US-Wavenet-A": "English (US) - Female (Wavenet)",
            "en-US-Wavenet-B": "English (US) - Male (Wavenet)",
            "en-US-Neural2-A": "English (US) - Female (Neural2)",
            "en-US-Neural2-B": "English (US) - Male (Neural2)",
        }
        
        self.setup_ui()
        self.load_saved_settings()
        
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from file"""
        config_file = "config.json"
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {}
    
    def save_config(self):
        """Save configuration to file"""
        try:
            with open("config.json", 'w') as f:
                json.dump(self.config, f, indent=2)
        except Exception as e:
            print(f"Error saving config: {e}")
    
    def setup_ui(self):
        """Setup the main user interface"""
        try:
            # Create main container with scrollable frame
            main_frame = ctk.CTkScrollableFrame(self.root)
            main_frame.pack(fill="both", expand=True, padx=20, pady=20)
            
            # Title
            title = ctk.CTkLabel(main_frame, text="Advanced Text-to-Speech with Gemini AI", 
                                font=ctk.CTkFont(size=24, weight="bold"))
            title.pack(pady=(0, 20))
            
            # API Configuration Section
            self.setup_api_section(main_frame)
            
            # TTS Service Selection
            self.setup_service_section(main_frame)
            
            # Model Selection Section
            self.setup_model_section(main_frame)
            
            # Text Input Section
            self.setup_text_section(main_frame)
            
            # Gemini AI Parameters Section
            self.setup_gemini_params_section(main_frame)
            
            # TTS Configuration Section
            self.setup_tts_section(main_frame)
            
            # Output Section
            self.setup_output_section(main_frame)
            
            # Control Buttons
            self.setup_controls(main_frame)
            
            # Status Section
            self.setup_status_section(main_frame)
            
        except Exception as e:
            print(f"\n❌ UI SETUP ERROR:")
            print(f"Error: {str(e)}")
            print(f"\nFull traceback:")
            import traceback
            traceback.print_exc()
            raise
    
    def setup_api_section(self, parent):
        """Setup API configuration section"""
        api_frame = ctk.CTkFrame(parent)
        api_frame.pack(fill="x", pady=(0, 20))
        
        api_label = ctk.CTkLabel(api_frame, text="🔑 API Configuration", 
                                font=ctk.CTkFont(size=18, weight="bold"))
        api_label.pack(pady=(15, 10))
        
        # Gemini API Key
        gemini_frame = ctk.CTkFrame(api_frame)
        gemini_frame.pack(fill="x", padx=15, pady=5)
        
        ctk.CTkLabel(gemini_frame, text="Gemini API Key:", 
                    font=ctk.CTkFont(size=12, weight="bold")).pack(anchor="w", padx=10, pady=(10, 5))
        
        self.gemini_api_entry = ctk.CTkEntry(gemini_frame, show="*", width=400)
        self.gemini_api_entry.pack(fill="x", padx=10, pady=(0, 5))
        
        # Add keyboard shortcuts for API key entry
        self._bind_entry_shortcuts(self.gemini_api_entry)
        
        ctk.CTkLabel(gemini_frame, text="Get your API key from: https://ai.google.dev/gemini-api", 
                    font=ctk.CTkFont(size=10), text_color="gray").pack(anchor="w", padx=10, pady=(0, 10))
        
        # Google Cloud TTS Configuration (conditionally shown)
        self.gcloud_frame = ctk.CTkFrame(api_frame)
        self.gcloud_frame.pack(fill="x", padx=15, pady=(5, 15))
        
        ctk.CTkLabel(self.gcloud_frame, text="Google Cloud TTS (for Google Cloud TTS service):", 
                    font=ctk.CTkFont(size=12, weight="bold")).pack(anchor="w", padx=10, pady=(10, 5))
        
        self.service_account_entry = ctk.CTkEntry(self.gcloud_frame, width=300)
        self.service_account_entry.pack(side="left", fill="x", expand=True, padx=(10, 5), pady=(0, 5))
        
        # Add keyboard shortcuts for service account entry
        self._bind_entry_shortcuts(self.service_account_entry)
        
        ctk.CTkButton(self.gcloud_frame, text="Browse", command=self.browse_service_account, 
                     width=80).pack(side="right", padx=(5, 10), pady=(0, 5))
        
        ctk.CTkLabel(self.gcloud_frame, text="Leave empty to use default credentials (only needed for Google Cloud TTS)", 
                    font=ctk.CTkFont(size=10), text_color="gray").pack(anchor="w", padx=10, pady=(0, 10))
    
    def setup_service_section(self, parent):
        """Setup TTS service selection section"""
        service_frame = ctk.CTkFrame(parent)
        service_frame.pack(fill="x", pady=(0, 20))
        
        service_label = ctk.CTkLabel(service_frame, text="🎵 TTS Service Selection", 
                                    font=ctk.CTkFont(size=18, weight="bold"))
        service_label.pack(pady=(15, 10))
        
        service_select_frame = ctk.CTkFrame(service_frame)
        service_select_frame.pack(fill="x", padx=15, pady=(0, 15))
        
        ctk.CTkLabel(service_select_frame, text="Choose TTS Service:", 
                    font=ctk.CTkFont(size=12, weight="bold")).pack(anchor="w", padx=10, pady=(10, 5))
        
        self.tts_service_var = ctk.StringVar(value=self.tts_services[0])
        self.tts_service_combo = ctk.CTkComboBox(service_select_frame, values=self.tts_services, 
                                                variable=self.tts_service_var, width=300,
                                                command=self.on_service_change)
        self.tts_service_combo.pack(anchor="w", padx=10, pady=(0, 5))
        
        ctk.CTkLabel(service_select_frame, 
                    text="• Gemini Native TTS: Direct audio generation from Gemini models (Latest!)\n"
                         "• Google Cloud TTS: Traditional cloud-based text-to-speech service", 
                    font=ctk.CTkFont(size=10), text_color="gray", 
                    justify="left").pack(anchor="w", padx=10, pady=(0, 10))
    
    def setup_model_section(self, parent):
        """Setup model selection section"""
        model_frame = ctk.CTkFrame(parent)
        model_frame.pack(fill="x", pady=(0, 20))
        
        model_label = ctk.CTkLabel(model_frame, text="🤖 Model Selection", 
                                  font=ctk.CTkFont(size=18, weight="bold"))
        model_label.pack(pady=(15, 10))
        
        model_select_frame = ctk.CTkFrame(model_frame)
        model_select_frame.pack(fill="x", padx=15, pady=(0, 15))
        
        # Text Enhancement Model
        ctk.CTkLabel(model_select_frame, text="Text Enhancement Model (Optional):", 
                    font=ctk.CTkFont(size=12, weight="bold")).pack(anchor="w", padx=10, pady=(10, 5))
        
        self.model_var = ctk.StringVar(value=self.gemini_models[0])
        self.model_combo = ctk.CTkComboBox(model_select_frame, values=self.gemini_models, 
                                          variable=self.model_var, width=300)
        self.model_combo.pack(anchor="w", padx=10, pady=(0, 5))
        
        # TTS Model (for Gemini Native TTS)
        self.tts_model_label = ctk.CTkLabel(model_select_frame, text="TTS Model:", 
                                           font=ctk.CTkFont(size=12, weight="bold"))
        self.tts_model_label.pack(anchor="w", padx=10, pady=(10, 5))
        
        self.tts_model_var = ctk.StringVar(value=self.gemini_tts_models[0])
        self.tts_model_combo = ctk.CTkComboBox(model_select_frame, values=self.gemini_tts_models, 
                                              variable=self.tts_model_var, width=300)
        self.tts_model_combo.pack(anchor="w", padx=10, pady=(0, 5))
        
        # Multi-speaker toggle
        self.multi_speaker_frame = ctk.CTkFrame(model_select_frame)
        self.multi_speaker_frame.pack(fill="x", padx=10, pady=5)
        
        self.use_multi_speaker_var = ctk.BooleanVar()
        self.multi_speaker_check = ctk.CTkCheckBox(self.multi_speaker_frame, 
                                                  text="Enable Multi-Speaker (2 speakers)", 
                                                  variable=self.use_multi_speaker_var,
                                                  command=self.toggle_multi_speaker)
        self.multi_speaker_check.pack(anchor="w", padx=10, pady=5)
        
        # Single speaker voice selection
        self.single_voice_frame = ctk.CTkFrame(model_select_frame)
        self.single_voice_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(self.single_voice_frame, text="Voice:", 
                    font=ctk.CTkFont(size=12, weight="bold")).pack(anchor="w", padx=10, pady=(10, 5))
        
        self.gemini_voice_var = ctk.StringVar(value=self.gemini_tts_voices[0])
        self.gemini_voice_combo = ctk.CTkComboBox(self.single_voice_frame, values=self.gemini_tts_voices, 
                                                 variable=self.gemini_voice_var, width=300)
        self.gemini_voice_combo.pack(anchor="w", padx=10, pady=(0, 10))
        
        # Multi-speaker voice selection (initially hidden)
        self.multi_voice_frame = ctk.CTkFrame(model_select_frame)
        
        ctk.CTkLabel(self.multi_voice_frame, text="Speaker 1 Voice:", 
                    font=ctk.CTkFont(size=11, weight="bold")).pack(anchor="w", padx=10, pady=(10, 2))
        
        self.speaker1_voice_var = ctk.StringVar(value="Kore")
        self.speaker1_voice_combo = ctk.CTkComboBox(self.multi_voice_frame, values=self.gemini_tts_voices, 
                                                   variable=self.speaker1_voice_var, width=250)
        self.speaker1_voice_combo.pack(anchor="w", padx=10, pady=(0, 5))
        
        ctk.CTkLabel(self.multi_voice_frame, text="Speaker 2 Voice:", 
                    font=ctk.CTkFont(size=11, weight="bold")).pack(anchor="w", padx=10, pady=(5, 2))
        
        self.speaker2_voice_var = ctk.StringVar(value="Puck")
        self.speaker2_voice_combo = ctk.CTkComboBox(self.multi_voice_frame, values=self.gemini_tts_voices, 
                                                   variable=self.speaker2_voice_var, width=250)
        self.speaker2_voice_combo.pack(anchor="w", padx=10, pady=(0, 10))
        
        ctk.CTkLabel(model_select_frame, 
                    text="• gemini-2.5-flash-preview-tts: Fast, low-latency TTS with good quality\n"
                         "• gemini-2.5-pro-preview-tts: Higher quality TTS with more advanced features", 
                    font=ctk.CTkFont(size=10), text_color="gray", 
                    justify="left").pack(anchor="w", padx=10, pady=(0, 10))
        
        # Update visibility based on initial service selection
        self.on_service_change(self.tts_service_var.get())
    
    def on_service_change(self, selected_service):
        """Handle TTS service change"""
        if selected_service == "Gemini Native TTS":
            # Show TTS model selection, hide Google Cloud config
            self.tts_model_label.pack(anchor="w", padx=10, pady=(10, 5))
            self.tts_model_combo.pack(anchor="w", padx=10, pady=(0, 5))
            self.multi_speaker_frame.pack(fill="x", padx=10, pady=5)
            # Show appropriate voice selection based on multi-speaker setting
            self.toggle_multi_speaker()
            self.gcloud_frame.pack_forget()
            
            # Hide Google Cloud TTS configuration in TTS section
            if hasattr(self, 'gcloud_tts_frame'):
                self.gcloud_tts_frame.pack_forget()
                
        else:  # Google Cloud TTS
            # Hide TTS model selection, show Google Cloud config
            self.tts_model_label.pack_forget()
            self.tts_model_combo.pack_forget()
            self.multi_speaker_frame.pack_forget()
            self.single_voice_frame.pack_forget()
            self.multi_voice_frame.pack_forget()
            self.gcloud_frame.pack(fill="x", padx=15, pady=(5, 15))
            
            # Show Google Cloud TTS configuration in TTS section
            if hasattr(self, 'gcloud_tts_frame'):
                self.gcloud_tts_frame.pack(fill="x", padx=10, pady=5)
    
    def setup_text_section(self, parent):
        """Setup text input section"""
        text_frame = ctk.CTkFrame(parent)
        text_frame.pack(fill="both", expand=True, pady=(0, 20))
        
        text_label = ctk.CTkLabel(text_frame, text="📝 Text Input", 
                                 font=ctk.CTkFont(size=18, weight="bold"))
        text_label.pack(pady=(15, 10))
        
        # Text input area
        input_frame = ctk.CTkFrame(text_frame)
        input_frame.pack(fill="both", expand=True, padx=15, pady=(0, 15))
        
        ctk.CTkLabel(input_frame, text="Enter your text:", 
                    font=ctk.CTkFont(size=12, weight="bold")).pack(anchor="w", padx=10, pady=(10, 5))
        
        self.text_input = ctk.CTkTextbox(input_frame, height=120)
        self.text_input.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Add keyboard shortcuts for text operations
        self._bind_text_shortcuts(self.text_input)
        
        # Add placeholder text manually
        placeholder_text = "Enter the text you want to convert to speech..."
        self.text_input.insert("1.0", placeholder_text)
        self.text_input.configure(text_color="gray")
        
        # Bind events for placeholder behavior
        self.text_input.bind("<FocusIn>", self._on_text_focus_in)
        self.text_input.bind("<FocusOut>", self._on_text_focus_out)
        self._placeholder_text = placeholder_text
        self._is_placeholder_active = True
        
        # Context/Instructions field
        context_frame = ctk.CTkFrame(input_frame)
        context_frame.pack(fill="x", padx=10, pady=(5, 10))
        
        ctk.CTkLabel(context_frame, text="Context/Instructions (Optional):", 
                    font=ctk.CTkFont(size=12, weight="bold")).pack(anchor="w", padx=10, pady=(10, 5))
        
        self.context_input = ctk.CTkTextbox(context_frame, height=80)
        self.context_input.pack(fill="x", padx=10, pady=(0, 5))
        
        # Add keyboard shortcuts for context input
        self._bind_text_shortcuts(self.context_input)
        
        # Add placeholder for context
        context_placeholder = "Add context, speaker names, or special instructions here..."
        self.context_input.insert("1.0", context_placeholder)
        self.context_input.configure(text_color="gray")
        
        # Bind events for context placeholder
        self.context_input.bind("<FocusIn>", self._on_context_focus_in)
        self.context_input.bind("<FocusOut>", self._on_context_focus_out)
        self._context_placeholder_text = context_placeholder
        self._is_context_placeholder_active = True
        
        ctk.CTkLabel(context_frame, 
                    text="Examples:\n"
                         "• Single Speaker: 'Say this in a cheerful, upbeat voice'\n"
                         "• Multi-Speaker: 'Speaker1: Hello there! Speaker2: Nice to meet you!'\n"
                         "• Context: 'This is a conversation between two friends at a coffee shop'", 
                    font=ctk.CTkFont(size=10), text_color="gray", 
                    justify="left").pack(anchor="w", padx=10, pady=(0, 10))
    
    def setup_gemini_params_section(self, parent):
        """Setup Gemini AI parameters section"""
        params_frame = ctk.CTkFrame(parent)
        params_frame.pack(fill="x", pady=(0, 20))
        
        params_label = ctk.CTkLabel(params_frame, text="⚙️ Gemini AI Parameters (Optional)", 
                                   font=ctk.CTkFont(size=18, weight="bold"))
        params_label.pack(pady=(15, 10))
        
        # Enable AI processing checkbox
        self.use_ai_var = ctk.BooleanVar()
        self.use_ai_check = ctk.CTkCheckBox(params_frame, text="Enhance text with Gemini AI before TTS", 
                                           variable=self.use_ai_var, command=self.toggle_ai_params)
        self.use_ai_check.pack(padx=15, pady=(0, 10))
        
        # AI parameters frame (initially hidden)
        self.ai_params_frame = ctk.CTkFrame(params_frame)
        
        # System prompt
        prompt_frame = ctk.CTkFrame(self.ai_params_frame)
        prompt_frame.pack(fill="x", padx=15, pady=5)
        
        ctk.CTkLabel(prompt_frame, text="System Prompt:", 
                    font=ctk.CTkFont(size=12, weight="bold")).pack(anchor="w", padx=10, pady=(10, 5))
        
        self.system_prompt = ctk.CTkTextbox(prompt_frame, height=80)
        self.system_prompt.pack(fill="x", padx=10, pady=(0, 5))
        
        # Add keyboard shortcuts for system prompt
        self._bind_text_shortcuts(self.system_prompt)
        
        # Add placeholder text for system prompt
        prompt_placeholder = "Optional: Add instructions for how AI should process your text..."
        self.system_prompt.insert("1.0", prompt_placeholder)
        self.system_prompt.configure(text_color="gray")
        
        # Bind events for system prompt placeholder
        self.system_prompt.bind("<FocusIn>", self._on_prompt_focus_in)
        self.system_prompt.bind("<FocusOut>", self._on_prompt_focus_out)
        self._prompt_placeholder_text = prompt_placeholder
        self._is_prompt_placeholder_active = True
        
        ctk.CTkLabel(prompt_frame, 
                    text="Example: 'Improve the clarity and flow of this text for speech synthesis'", 
                    font=ctk.CTkFont(size=10), text_color="gray").pack(anchor="w", padx=10, pady=(0, 10))
        
        # Generation parameters
        gen_frame = ctk.CTkFrame(self.ai_params_frame)
        gen_frame.pack(fill="x", padx=15, pady=(5, 15))
        
        # Temperature
        temp_frame = ctk.CTkFrame(gen_frame)
        temp_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(temp_frame, text="Temperature (0.0-2.0):", 
                    font=ctk.CTkFont(size=11)).pack(side="left", padx=(10, 5))
        self.temp_var = ctk.DoubleVar(value=0.7)
        self.temp_slider = ctk.CTkSlider(temp_frame, from_=0.0, to=2.0, variable=self.temp_var, 
                                        number_of_steps=20)
        self.temp_slider.pack(side="left", fill="x", expand=True, padx=5)
        self.temp_label = ctk.CTkLabel(temp_frame, text="0.7")
        self.temp_label.pack(side="right", padx=(5, 10))
        self.temp_slider.configure(command=lambda v: self.temp_label.configure(text=f"{v:.1f}"))
        
        # Max tokens
        tokens_frame = ctk.CTkFrame(gen_frame)
        tokens_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(tokens_frame, text="Max Output Tokens:", 
                    font=ctk.CTkFont(size=11)).pack(side="left", padx=(10, 5))
        self.max_tokens_var = ctk.IntVar(value=1000)
        self.max_tokens_entry = ctk.CTkEntry(tokens_frame, textvariable=self.max_tokens_var, width=100)
        self.max_tokens_entry.pack(side="right", padx=(5, 10))
    
    def setup_tts_section(self, parent):
        """Setup TTS configuration section"""
        tts_frame = ctk.CTkFrame(parent)
        tts_frame.pack(fill="x", pady=(0, 20))
        
        tts_label = ctk.CTkLabel(tts_frame, text="🔊 Text-to-Speech Configuration", 
                                font=ctk.CTkFont(size=18, weight="bold"))
        tts_label.pack(pady=(15, 10))
        
        config_frame = ctk.CTkFrame(tts_frame)
        config_frame.pack(fill="x", padx=15, pady=(0, 15))
        
        # Google Cloud TTS Configuration (shown conditionally)
        self.gcloud_tts_frame = ctk.CTkFrame(config_frame)
        
        # Voice selection
        voice_frame = ctk.CTkFrame(self.gcloud_tts_frame)
        voice_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(voice_frame, text="Voice:", 
                    font=ctk.CTkFont(size=12, weight="bold")).pack(anchor="w", padx=10, pady=(10, 5))
        
        self.voice_var = ctk.StringVar(value="en-US-Neural2-A")
        voice_display = [f"{k}: {v}" for k, v in self.voice_options.items()]
        self.voice_combo = ctk.CTkComboBox(voice_frame, values=voice_display, width=400)
        self.voice_combo.pack(anchor="w", padx=10, pady=(0, 10))
        
        # Speaking rate and pitch
        audio_params_frame = ctk.CTkFrame(self.gcloud_tts_frame)
        audio_params_frame.pack(fill="x", padx=10, pady=5)
        
        # Speaking rate
        rate_frame = ctk.CTkFrame(audio_params_frame)
        rate_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(rate_frame, text="Speaking Rate (0.25-4.0):", 
                    font=ctk.CTkFont(size=11)).pack(side="left", padx=(10, 5))
        self.rate_var = ctk.DoubleVar(value=1.0)
        self.rate_slider = ctk.CTkSlider(rate_frame, from_=0.25, to=4.0, variable=self.rate_var, 
                                        number_of_steps=15)
        self.rate_slider.pack(side="left", fill="x", expand=True, padx=5)
        self.rate_label = ctk.CTkLabel(rate_frame, text="1.0")
        self.rate_label.pack(side="right", padx=(5, 10))
        self.rate_slider.configure(command=lambda v: self.rate_label.configure(text=f"{v:.2f}"))
        
        # Pitch
        pitch_frame = ctk.CTkFrame(audio_params_frame)
        pitch_frame.pack(fill="x", padx=10, pady=(5, 10))
        
        ctk.CTkLabel(pitch_frame, text="Pitch (-20.0 to 20.0):", 
                    font=ctk.CTkFont(size=11)).pack(side="left", padx=(10, 5))
        self.pitch_var = ctk.DoubleVar(value=0.0)
        self.pitch_slider = ctk.CTkSlider(pitch_frame, from_=-20.0, to=20.0, variable=self.pitch_var, 
                                         number_of_steps=40)
        self.pitch_slider.pack(side="left", fill="x", expand=True, padx=5)
        self.pitch_label = ctk.CTkLabel(pitch_frame, text="0.0")
        self.pitch_label.pack(side="right", padx=(5, 10))
        self.pitch_slider.configure(command=lambda v: self.pitch_label.configure(text=f"{v:.1f}"))
        
        # Gemini Native TTS info
        gemini_info_frame = ctk.CTkFrame(config_frame)
        gemini_info_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(gemini_info_frame, 
                    text="ℹ️ Gemini Native TTS Configuration:\n"
                         "• Single & Multi-Speaker support (up to 2 speakers)\n"
                         "• Voice characteristics controlled by voice selection and text prompts\n"
                         "• Use context field for speaker instructions and dialogue setup\n"
                         "• Example: 'Speaker1: Hello! Speaker2: Nice to meet you!'\n"
                         "• Over 30 voice options available (Kore, Puck, Zephyr, etc.)\n"
                         "• Output format: WAV (24kHz, 16-bit, mono)", 
                    font=ctk.CTkFont(size=10), text_color="lightblue", 
                    justify="left").pack(anchor="w", padx=10, pady=10)
    
    def setup_output_section(self, parent):
        """Setup output configuration section"""
        output_frame = ctk.CTkFrame(parent)
        output_frame.pack(fill="x", pady=(0, 20))
        
        output_label = ctk.CTkLabel(output_frame, text="💾 Output Configuration", 
                                   font=ctk.CTkFont(size=18, weight="bold"))
        output_label.pack(pady=(15, 10))
        
        file_frame = ctk.CTkFrame(output_frame)
        file_frame.pack(fill="x", padx=15, pady=(0, 15))
        
        ctk.CTkLabel(file_frame, text="Output Filename:", 
                    font=ctk.CTkFont(size=12, weight="bold")).pack(anchor="w", padx=10, pady=(10, 5))
        
        filename_frame = ctk.CTkFrame(file_frame)
        filename_frame.pack(fill="x", padx=10, pady=(0, 5))
        
        self.filename_entry = ctk.CTkEntry(filename_frame, width=300)
        self.filename_entry.pack(side="left", fill="x", expand=True, padx=(10, 5))
        
        # Add keyboard shortcuts for filename entry
        self._bind_entry_shortcuts(self.filename_entry)
        
        # Add default filename
        self.filename_entry.insert(0, "output_audio.mp3")
        
        ctk.CTkButton(filename_frame, text="Browse", command=self.browse_output_file, 
                     width=80).pack(side="right", padx=(5, 10))
        
        ctk.CTkLabel(file_frame, text="Supported formats: .mp3, .wav (Gemini TTS outputs WAV, Google Cloud TTS supports both)", 
                    font=ctk.CTkFont(size=10), text_color="gray").pack(anchor="w", padx=10, pady=(0, 10))
    
    def setup_controls(self, parent):
        """Setup control buttons"""
        controls_frame = ctk.CTkFrame(parent)
        controls_frame.pack(fill="x", pady=(0, 20))
        
        buttons_frame = ctk.CTkFrame(controls_frame)
        buttons_frame.pack(pady=20)
        
        # Generate button
        self.generate_btn = ctk.CTkButton(buttons_frame, text="🎵 Generate Speech", 
                                         command=self.generate_speech, width=150, height=40,
                                         font=ctk.CTkFont(size=14, weight="bold"))
        self.generate_btn.pack(side="left", padx=10)
        
        # Play button
        self.play_btn = ctk.CTkButton(buttons_frame, text="▶️ Play Audio", 
                                     command=self.play_audio, width=120, height=40,
                                     state="disabled")
        self.play_btn.pack(side="left", padx=10)
        
        # Stop button
        self.stop_btn = ctk.CTkButton(buttons_frame, text="⏹️ Stop", 
                                     command=self.stop_audio, width=100, height=40,
                                     state="disabled")
        self.stop_btn.pack(side="left", padx=10)
        
        # Save settings button
        ctk.CTkButton(buttons_frame, text="💾 Save Settings", 
                     command=self.save_settings, width=120, height=40).pack(side="left", padx=10)
    
    def setup_status_section(self, parent):
        """Setup status section"""
        status_frame = ctk.CTkFrame(parent)
        status_frame.pack(fill="x", pady=(0, 20))
        
        ctk.CTkLabel(status_frame, text="📊 Status", 
                    font=ctk.CTkFont(size=18, weight="bold")).pack(pady=(15, 10))
        
        self.status_text = ctk.CTkTextbox(status_frame, height=100)
        self.status_text.pack(fill="x", padx=15, pady=(0, 15))
        
        self.update_status("Ready to generate speech. Please configure your API keys.")
    
    def toggle_ai_params(self):
        """Toggle AI parameters visibility"""
        if self.use_ai_var.get():
            self.ai_params_frame.pack(fill="x", padx=15, pady=(0, 15))
        else:
            self.ai_params_frame.pack_forget()
    
    def _on_text_focus_in(self, event):
        """Handle text input focus in (remove placeholder)"""
        if self._is_placeholder_active:
            self.text_input.delete("1.0", tk.END)
            self.text_input.configure(text_color="white")
            self._is_placeholder_active = False
    
    def _on_text_focus_out(self, event):
        """Handle text input focus out (add placeholder if empty)"""
        if not self.text_input.get("1.0", tk.END).strip():
            self.text_input.delete("1.0", tk.END)
            self.text_input.insert("1.0", self._placeholder_text)
            self.text_input.configure(text_color="gray")
            self._is_placeholder_active = True
    
    def _get_text_input(self):
        """Get actual text input (excluding placeholder)"""
        if self._is_placeholder_active:
            return ""
        return self.text_input.get("1.0", tk.END).strip()
    
    def _on_prompt_focus_in(self, event):
        """Handle system prompt focus in (remove placeholder)"""
        if self._is_prompt_placeholder_active:
            self.system_prompt.delete("1.0", tk.END)
            self.system_prompt.configure(text_color="white")
            self._is_prompt_placeholder_active = False
    
    def _on_prompt_focus_out(self, event):
        """Handle system prompt focus out (add placeholder if empty)"""
        if not self.system_prompt.get("1.0", tk.END).strip():
            self.system_prompt.delete("1.0", tk.END)
            self.system_prompt.insert("1.0", self._prompt_placeholder_text)
            self.system_prompt.configure(text_color="gray")
            self._is_prompt_placeholder_active = True
    
    def _get_system_prompt(self):
        """Get actual system prompt (excluding placeholder)"""
        if self._is_prompt_placeholder_active:
            return ""
        return self.system_prompt.get("1.0", tk.END).strip()
    
    def _bind_text_shortcuts(self, widget):
        """Bind keyboard shortcuts for text widgets (CTkTextbox)"""
        # Ctrl+A - Select All
        widget.bind("<Control-a>", lambda e: self._select_all_text(widget))
        widget.bind("<Control-A>", lambda e: self._select_all_text(widget))
        
        # Ctrl+C - Copy
        widget.bind("<Control-c>", lambda e: self._copy_text(widget))
        widget.bind("<Control-C>", lambda e: self._copy_text(widget))
        
        # Ctrl+V - Paste
        widget.bind("<Control-v>", lambda e: self._paste_text(widget))
        widget.bind("<Control-V>", lambda e: self._paste_text(widget))
        
        # Ctrl+X - Cut
        widget.bind("<Control-x>", lambda e: self._cut_text(widget))
        widget.bind("<Control-X>", lambda e: self._cut_text(widget))
        
        # Ctrl+Z - Undo (if supported)
        widget.bind("<Control-z>", lambda e: self._undo_text(widget))
        widget.bind("<Control-Z>", lambda e: self._undo_text(widget))
    
    def _bind_entry_shortcuts(self, widget):
        """Bind keyboard shortcuts for entry widgets (CTkEntry)"""
        # Ctrl+A - Select All
        widget.bind("<Control-a>", lambda e: self._select_all_entry(widget))
        widget.bind("<Control-A>", lambda e: self._select_all_entry(widget))
        
        # Ctrl+C - Copy
        widget.bind("<Control-c>", lambda e: self._copy_entry(widget))
        widget.bind("<Control-C>", lambda e: self._copy_entry(widget))
        
        # Ctrl+V - Paste
        widget.bind("<Control-v>", lambda e: self._paste_entry(widget))
        widget.bind("<Control-V>", lambda e: self._paste_entry(widget))
        
        # Ctrl+X - Cut
        widget.bind("<Control-x>", lambda e: self._cut_entry(widget))
        widget.bind("<Control-X>", lambda e: self._cut_entry(widget))
    
    def _select_all_text(self, widget):
        """Select all text in a text widget"""
        try:
            widget.tag_add(tk.SEL, "1.0", tk.END)
            widget.mark_set(tk.INSERT, "1.0")
            widget.see(tk.INSERT)
            return "break"
        except:
            pass
    
    def _copy_text(self, widget):
        """Copy selected text from text widget"""
        try:
            selected_text = widget.selection_get()
            widget.clipboard_clear()
            widget.clipboard_append(selected_text)
            return "break"
        except:
            pass
    
    def _paste_text(self, widget):
        """Paste text into text widget"""
        try:
            # Handle placeholder text
            if widget == self.text_input and self._is_placeholder_active:
                self.text_input.delete("1.0", tk.END)
                self.text_input.configure(text_color="white")
                self._is_placeholder_active = False
            elif widget == self.system_prompt and self._is_prompt_placeholder_active:
                self.system_prompt.delete("1.0", tk.END)
                self.system_prompt.configure(text_color="white")
                self._is_prompt_placeholder_active = False
            elif widget == self.context_input and self._is_context_placeholder_active:
                self.context_input.delete("1.0", tk.END)
                self.context_input.configure(text_color="white")
                self._is_context_placeholder_active = False
            
            clipboard_text = widget.clipboard_get()
            widget.insert(tk.INSERT, clipboard_text)
            return "break"
        except:
            pass
    
    def _cut_text(self, widget):
        """Cut selected text from text widget"""
        try:
            selected_text = widget.selection_get()
            widget.clipboard_clear()
            widget.clipboard_append(selected_text)
            widget.delete(tk.SEL_FIRST, tk.SEL_LAST)
            return "break"
        except:
            pass
    
    def _undo_text(self, widget):
        """Undo last action in text widget"""
        try:
            widget.edit_undo()
            return "break"
        except:
            pass
    
    def _select_all_entry(self, widget):
        """Select all text in an entry widget"""
        try:
            widget.select_range(0, tk.END)
            widget.icursor(tk.END)
            return "break"
        except:
            pass
    
    def _copy_entry(self, widget):
        """Copy selected text from entry widget"""
        try:
            if widget.selection_present():
                selected_text = widget.selection_get()
                widget.clipboard_clear()
                widget.clipboard_append(selected_text)
            return "break"
        except:
            pass
    
    def _paste_entry(self, widget):
        """Paste text into entry widget"""
        try:
            clipboard_text = widget.clipboard_get()
            if widget.selection_present():
                widget.delete(tk.SEL_FIRST, tk.SEL_LAST)
            widget.insert(tk.INSERT, clipboard_text)
            return "break"
        except:
            pass
    
    def _cut_entry(self, widget):
        """Cut selected text from entry widget"""
        try:
            if widget.selection_present():
                selected_text = widget.selection_get()
                widget.clipboard_clear()
                widget.clipboard_append(selected_text)
                widget.delete(tk.SEL_FIRST, tk.SEL_LAST)
            return "break"
        except:
            pass
    
    def browse_service_account(self):
        """Browse for service account JSON file"""
        filename = filedialog.askopenfilename(
            title="Select Service Account JSON File",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")]
        )
        if filename:
            self.service_account_entry.delete(0, tk.END)
            self.service_account_entry.insert(0, filename)
    
    def browse_output_file(self):
        """Browse for output file location"""
        filename = filedialog.asksaveasfilename(
            title="Save Audio File As",
            defaultextension=".mp3",
            filetypes=[("MP3 files", "*.mp3"), ("WAV files", "*.wav"), ("All files", "*.*")]
        )
        if filename:
            self.filename_entry.delete(0, tk.END)
            self.filename_entry.insert(0, filename)
    
    def update_status(self, message: str):
        """Update status text"""
        self.status_text.delete("1.0", tk.END)
        self.status_text.insert("1.0", f"[{self.get_timestamp()}] {message}")
    
    def get_timestamp(self) -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().strftime("%H:%M:%S")
    
    def initialize_clients(self) -> bool:
        """Initialize API clients"""
        try:
            # Initialize Gemini client
            gemini_key = self.gemini_api_entry.get().strip()
            if not gemini_key:
                self.update_status("Error: Gemini API key is required")
                return False
            
            genai.configure(api_key=gemini_key)
            
            # Initialize text enhancement client
            self.gemini_client = genai.GenerativeModel(self.model_var.get())
            
            # Initialize Gemini TTS client if using native TTS
            if self.tts_service_var.get() == "Gemini Native TTS":
                self.gemini_tts_client = genai.GenerativeModel(self.tts_model_var.get())
            else:
                # Initialize Google Cloud TTS client
                service_account_path = self.service_account_entry.get().strip()
                if service_account_path and os.path.exists(service_account_path):
                    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_path
                
                self.tts_client = texttospeech.TextToSpeechClient()
            
            self.update_status("API clients initialized successfully")
            return True
            
        except Exception as e:
            self.update_status(f"Error initializing clients: {str(e)}")
            return False
    
    def process_text_with_ai(self, text: str) -> str:
        """Process text with Gemini AI if enabled"""
        if not self.use_ai_var.get() or not text.strip():
            return text
        
        try:
            system_prompt = self._get_system_prompt()
            
            # Prepare the prompt
            if system_prompt:
                full_prompt = f"{system_prompt}\n\nText to process:\n{text}"
            else:
                full_prompt = f"Please improve the following text for clarity and flow for text-to-speech conversion:\n\n{text}"
            
            # Generate content with parameters
            generation_config = genai.types.GenerationConfig(
                temperature=self.temp_var.get(),
                max_output_tokens=self.max_tokens_var.get(),
            )
            
            response = self.gemini_client.generate_content(
                full_prompt,
                generation_config=generation_config
            )
            
            processed_text = response.text
            self.update_status(f"Text processed successfully with {self.model_var.get()}")
            return processed_text
            
        except Exception as e:
            self.update_status(f"Error processing text with AI: {str(e)}")
            return text
    
    def generate_speech_with_gemini(self, text: str, output_file: str):
        """Generate speech using Gemini Native TTS"""
        try:
            self.update_status("Generating speech with Gemini TTS...")
            
            # Check if we have the new google.genai library
            try:
                import google.genai as genai_new
            except ImportError:
                # Fall back to attempting with older approach, but show warning
                self.update_status("Warning: New google.genai library not found. Installing required package...")
                import subprocess
                import sys
                
                # Try to install the new library
                try:
                    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'google-genai'])
                    import google.genai as genai_new
                    self.update_status("Successfully installed google-genai library")
                except Exception as install_error:
                    raise Exception(f"Cannot install google-genai library: {install_error}")
            
            # Run the async TTS generation
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(self._generate_gemini_tts_async(genai_new, text, output_file))
            finally:
                loop.close()
                
        except Exception as e:
            error_msg = str(e)
            
            # Provide helpful error messages for common issues
            if "not found for API version" in error_msg:
                raise Exception(f"Gemini TTS model not available. The model '{self.tts_model_var.get()}' may not be accessible with your API key or may be in limited preview. Try using Google Cloud TTS instead.")
            elif "policy violation" in error_msg:
                raise Exception(f"API access issue. Your Gemini API key may not have access to TTS preview features. Please check your API key permissions or try Google Cloud TTS.")
            elif "google-genai" in error_msg:
                raise Exception(f"Library installation failed. Please manually install: pip install google-genai")
            else:
                raise Exception(f"Gemini TTS error: {error_msg}")
    
    async def _generate_gemini_tts_async(self, genai_new, text: str, output_file: str):
        """Async method to generate TTS using Gemini API"""
        try:
            # Initialize client with new library
            client = genai_new.Client(
                api_key=self.gemini_api_entry.get().strip()
            )
            
            # Ensure output file has .wav extension for Gemini TTS
            if not output_file.lower().endswith('.wav'):
                output_file = output_file.rsplit('.', 1)[0] + '.wav'
            
            # Get context and combine with text
            context = self._get_context_input()
            if context:
                # Combine context with main text
                combined_text = f"{context}\n\n{text}"
            else:
                combined_text = text
            
            # Configure speech settings based on multi-speaker option
            if self.use_multi_speaker_var.get():
                # Multi-speaker configuration
                speech_config = genai_new.types.SpeechConfig(
                    multi_speaker_voice_config=genai_new.types.MultiSpeakerVoiceConfig(
                        speaker_voice_configs=[
                            genai_new.types.SpeakerVoiceConfig(
                                speaker='Speaker1',
                                voice_config=genai_new.types.VoiceConfig(
                                    prebuilt_voice_config=genai_new.types.PrebuiltVoiceConfig(
                                        voice_name=self.speaker1_voice_var.get()
                                    )
                                )
                            ),
                            genai_new.types.SpeakerVoiceConfig(
                                speaker='Speaker2',
                                voice_config=genai_new.types.VoiceConfig(
                                    prebuilt_voice_config=genai_new.types.PrebuiltVoiceConfig(
                                        voice_name=self.speaker2_voice_var.get()
                                    )
                                )
                            )
                        ]
                    )
                )
            else:
                # Single speaker configuration
                speech_config = genai_new.types.SpeechConfig(
                    voice_config=genai_new.types.VoiceConfig(
                        prebuilt_voice_config=genai_new.types.PrebuiltVoiceConfig(
                            voice_name=self.gemini_voice_var.get()
                        )
                    )
                )
            
            # Use the standard generate_content method with speech config for TTS
            response = await client.aio.models.generate_content(
                model=self.tts_model_var.get(),
                contents=combined_text,
                config=genai_new.types.GenerateContentConfig(
                    response_modalities=["AUDIO"],
                    speech_config=speech_config
                )
            )
            
            # Extract audio data
            audio_data = response.candidates[0].content.parts[0].inline_data.data
            
            # Set up WAV file for writing - Gemini returns PCM data
            with wave.open(output_file, 'wb') as wf:
                wf.setnchannels(1)  # Mono
                wf.setsampwidth(2)  # 16-bit
                wf.setframerate(24000)  # 24kHz
                wf.writeframes(audio_data)
            
            return output_file
            
        except Exception as e:
            # More specific error handling
            error_msg = str(e)
            if "models/" in error_msg and "not found" in error_msg:
                raise Exception(f"TTS model '{self.tts_model_var.get()}' is not available. This may be a preview feature with limited access.")
            elif "voice_name" in error_msg:
                raise Exception(f"Voice '{self.gemini_voice_var.get()}' is not available. Please try a different voice.")
            else:
                raise Exception(f"TTS generation failed: {error_msg}")
    
    def generate_speech_with_google_cloud(self, text: str, output_file: str):
        """Generate speech using Google Cloud TTS"""
        try:
            self.update_status("Generating speech with Google Cloud TTS...")
            
            # Get selected voice
            voice_selection = self.voice_combo.get()
            voice_name = voice_selection.split(':')[0] if ':' in voice_selection else "en-US-Neural2-A"
            
            # Configure TTS request
            synthesis_input = texttospeech.SynthesisInput(text=text)
            
            voice = texttospeech.VoiceSelectionParams(
                language_code="en-US",
                name=voice_name
            )
            
            # Determine audio format
            if output_file.lower().endswith('.wav'):
                audio_config = texttospeech.AudioConfig(
                    audio_encoding=texttospeech.AudioEncoding.LINEAR16,
                    speaking_rate=self.rate_var.get(),
                    pitch=self.pitch_var.get()
                )
            else:
                audio_config = texttospeech.AudioConfig(
                    audio_encoding=texttospeech.AudioEncoding.MP3,
                    speaking_rate=self.rate_var.get(),
                    pitch=self.pitch_var.get()
                )
            
            # Generate speech
            response = self.tts_client.synthesize_speech(
                input=synthesis_input,
                voice=voice,
                audio_config=audio_config
            )
            
            # Save audio file
            with open(output_file, "wb") as out:
                out.write(response.audio_content)
            
            return output_file
            
        except Exception as e:
            raise Exception(f"Google Cloud TTS error: {str(e)}")
    
    def generate_speech(self):
        """Generate speech from text"""
        def worker():
            try:
                # Disable generate button
                self.generate_btn.configure(state="disabled", text="Generating...")
                
                # Get input text
                input_text = self._get_text_input()
                if not input_text:
                    self.update_status("Error: Please enter some text")
                    return
                
                # Initialize clients
                if not self.initialize_clients():
                    return
                
                self.update_status("Processing text...")
                
                # Process text with AI if enabled
                processed_text = self.process_text_with_ai(input_text)
                
                # Get output filename
                output_file = self.filename_entry.get().strip()
                if not output_file:
                    output_file = "output_audio.mp3"
                
                # Generate speech based on selected service
                if self.tts_service_var.get() == "Gemini Native TTS":
                    output_file = self.generate_speech_with_gemini(processed_text, output_file)
                else:
                    # Ensure proper file extension for Google Cloud TTS
                    if not output_file.lower().endswith(('.mp3', '.wav')):
                        output_file += '.mp3'
                    output_file = self.generate_speech_with_google_cloud(processed_text, output_file)
                
                self.current_audio_file = output_file
                self.play_btn.configure(state="normal")
                
                service_name = self.tts_service_var.get()
                if service_name == "Gemini Native TTS":
                    model_info = f" using {self.tts_model_var.get()}"
                else:
                    model_info = ""
                
                self.update_status(f"Speech generated successfully with {service_name}{model_info} and saved as: {output_file}")
                
            except Exception as e:
                error_msg = f"Error generating speech: {str(e)}"
                self.update_status(error_msg)
                print(f"\n❌ TTS GENERATION ERROR:")
                print(f"Error: {str(e)}")
                print(f"\nFull traceback:")
                import traceback
                traceback.print_exc()
                
            finally:
                # Re-enable generate button
                self.generate_btn.configure(state="normal", text="🎵 Generate Speech")
        
        # Run in separate thread to prevent UI blocking
        threading.Thread(target=worker, daemon=True).start()
    
    def play_audio(self):
        """Play the generated audio"""
        try:
            if hasattr(self, 'current_audio_file') and os.path.exists(self.current_audio_file):
                pygame.mixer.music.load(self.current_audio_file)
                pygame.mixer.music.play()
                self.stop_btn.configure(state="normal")
                self.update_status(f"Playing: {self.current_audio_file}")
            else:
                self.update_status("No audio file to play")
        except Exception as e:
            self.update_status(f"Error playing audio: {str(e)}")
    
    def stop_audio(self):
        """Stop audio playback"""
        try:
            pygame.mixer.music.stop()
            self.stop_btn.configure(state="disabled")
            self.update_status("Audio playback stopped")
        except Exception as e:
            self.update_status(f"Error stopping audio: {str(e)}")
    
    def save_settings(self):
        """Save current settings"""
        try:
            settings = {
                'tts_service': self.tts_service_var.get(),
                'gemini_model': self.model_var.get(),
                'tts_model': self.tts_model_var.get(),
                'gemini_voice': self.gemini_voice_var.get(),
                'use_multi_speaker': self.use_multi_speaker_var.get(),
                'speaker1_voice': self.speaker1_voice_var.get(),
                'speaker2_voice': self.speaker2_voice_var.get(),
                'use_ai': self.use_ai_var.get(),
                'temperature': self.temp_var.get(),
                'max_tokens': self.max_tokens_var.get(),
                'voice': self.voice_combo.get(),
                'speaking_rate': self.rate_var.get(),
                'pitch': self.pitch_var.get(),
                'output_filename': self.filename_entry.get(),
                'system_prompt': self._get_system_prompt(),
                'context_input': self._get_context_input(),
                'service_account_path': self.service_account_entry.get()
            }
            
            with open('settings.json', 'w') as f:
                json.dump(settings, f, indent=2)
            
            self.update_status("Settings saved successfully")
            
        except Exception as e:
            self.update_status(f"Error saving settings: {str(e)}")
    
    def load_saved_settings(self):
        """Load saved settings"""
        try:
            if os.path.exists('settings.json'):
                with open('settings.json', 'r') as f:
                    settings = json.load(f)
                
                # Apply settings
                if 'tts_service' in settings:
                    self.tts_service_var.set(settings['tts_service'])
                    self.on_service_change(settings['tts_service'])
                if 'gemini_model' in settings:
                    self.model_var.set(settings['gemini_model'])
                if 'tts_model' in settings:
                    self.tts_model_var.set(settings['tts_model'])
                if 'gemini_voice' in settings:
                    self.gemini_voice_var.set(settings['gemini_voice'])
                if 'use_multi_speaker' in settings:
                    self.use_multi_speaker_var.set(settings['use_multi_speaker'])
                    self.toggle_multi_speaker()
                if 'speaker1_voice' in settings:
                    self.speaker1_voice_var.set(settings['speaker1_voice'])
                if 'speaker2_voice' in settings:
                    self.speaker2_voice_var.set(settings['speaker2_voice'])
                if 'use_ai' in settings:
                    self.use_ai_var.set(settings['use_ai'])
                    self.toggle_ai_params()
                if 'temperature' in settings:
                    self.temp_var.set(settings['temperature'])
                if 'max_tokens' in settings:
                    self.max_tokens_var.set(settings['max_tokens'])
                if 'voice' in settings:
                    self.voice_combo.set(settings['voice'])
                if 'speaking_rate' in settings:
                    self.rate_var.set(settings['speaking_rate'])
                if 'pitch' in settings:
                    self.pitch_var.set(settings['pitch'])
                if 'output_filename' in settings:
                    self.filename_entry.insert(0, settings['output_filename'])
                if 'system_prompt' in settings:
                    if settings['system_prompt'].strip():  # Only load non-empty prompts
                        if self._is_prompt_placeholder_active:
                            self.system_prompt.delete("1.0", tk.END)
                            self.system_prompt.configure(text_color="white")
                            self._is_prompt_placeholder_active = False
                        self.system_prompt.insert("1.0", settings['system_prompt'])
                if 'context_input' in settings:
                    if settings['context_input'].strip():  # Only load non-empty context
                        if self._is_context_placeholder_active:
                            self.context_input.delete("1.0", tk.END)
                            self.context_input.configure(text_color="white")
                            self._is_context_placeholder_active = False
                        self.context_input.insert("1.0", settings['context_input'])
                if 'service_account_path' in settings:
                    self.service_account_entry.insert(0, settings['service_account_path'])
                
                self.update_status("Settings loaded successfully")
                
        except Exception as e:
            self.update_status(f"Note: Could not load saved settings: {str(e)}")
    
    def toggle_multi_speaker(self):
        """Toggle multi-speaker voice selection visibility"""
        if self.use_multi_speaker_var.get():
            self.single_voice_frame.pack_forget()
            self.multi_voice_frame.pack(fill="x", padx=10, pady=5)
        else:
            self.multi_voice_frame.pack_forget()
            self.single_voice_frame.pack(fill="x", padx=10, pady=5)
    
    def _on_context_focus_in(self, event):
        """Handle context input focus in (remove placeholder)"""
        if self._is_context_placeholder_active:
            self.context_input.delete("1.0", tk.END)
            self.context_input.configure(text_color="white")
            self._is_context_placeholder_active = False
    
    def _on_context_focus_out(self, event):
        """Handle context input focus out (add placeholder if empty)"""
        if not self.context_input.get("1.0", tk.END).strip():
            self.context_input.delete("1.0", tk.END)
            self.context_input.insert("1.0", self._context_placeholder_text)
            self.context_input.configure(text_color="gray")
            self._is_context_placeholder_active = True
    
    def _get_context_input(self):
        """Get actual context input (excluding placeholder)"""
        if self._is_context_placeholder_active:
            return ""
        return self.context_input.get("1.0", tk.END).strip()
    
    def run(self):
        """Run the application"""
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.root.mainloop()
    
    def on_closing(self):
        """Handle application closing"""
        try:
            pygame.mixer.quit()
        except:
            pass
        self.root.destroy()

if __name__ == "__main__":
    try:
        app = TextToSpeechApp()
        app.run()
    except Exception as e:
        print(f"\n❌ APPLICATION ERROR:")
        print(f"Error: {str(e)}")
        print(f"\nFull traceback:")
        import traceback
        traceback.print_exc()
        print(f"\nPress Enter to exit...")
        input() 