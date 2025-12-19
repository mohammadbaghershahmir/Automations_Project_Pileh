import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
from pathlib import Path
import threading
from pydub import AudioSegment
from pydub.utils import which

class AudioTypeChangerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Audio Type Changer - Convert to MP3 64kbps")
        self.root.geometry("700x500")
        self.root.resizable(True, True)
        
        # Configure style
        self.style = ttk.Style()
        self.style.theme_use('clam')
        
        # Folder paths
        self.input_folder = ""
        self.output_folder = ""
        
        # List of audio files to process
        self.audio_files = []
        
        self.setup_ui()
        
    def setup_ui(self):
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(3, weight=1)
        
        # Title
        title_label = ttk.Label(main_frame, text="Audio Type Changer", 
                               font=('Arial', 16, 'bold'))
        title_label.grid(row=0, column=0, columnspan=3, pady=(0, 10))
        
        # Description
        desc_label = ttk.Label(main_frame, 
                              text="Converts all audio files to MP3 format at 64kbps",
                              font=('Arial', 9), foreground='gray')
        desc_label.grid(row=1, column=0, columnspan=3, pady=(0, 20))
        
        # Folder selection section
        folder_frame = ttk.LabelFrame(main_frame, text="Folder Selection", padding="10")
        folder_frame.grid(row=2, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10))
        folder_frame.columnconfigure(1, weight=1)
        
        # Input folder
        ttk.Label(folder_frame, text="Input Folder:").grid(row=0, column=0, sticky=tk.W, pady=(0, 10))
        
        input_frame = ttk.Frame(folder_frame)
        input_frame.grid(row=0, column=1, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        input_frame.columnconfigure(0, weight=1)
        
        self.input_var = tk.StringVar()
        self.input_entry = ttk.Entry(input_frame, textvariable=self.input_var)
        self.input_entry.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=(0, 10))
        
        ttk.Button(input_frame, text="Browse", 
                  command=self.select_input_folder).grid(row=0, column=1)
        
        # Output folder
        ttk.Label(folder_frame, text="Output Folder:").grid(row=1, column=0, sticky=tk.W, pady=(0, 10))
        
        output_frame = ttk.Frame(folder_frame)
        output_frame.grid(row=1, column=1, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        output_frame.columnconfigure(0, weight=1)
        
        self.output_var = tk.StringVar()
        self.output_entry = ttk.Entry(output_frame, textvariable=self.output_var)
        self.output_entry.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=(0, 10))
        
        ttk.Button(output_frame, text="Browse", 
                  command=self.select_output_folder).grid(row=0, column=1)
        
        # Scan button
        ttk.Button(folder_frame, text="Scan Input Folder", 
                  command=self.scan_input_folder).grid(row=2, column=0, columnspan=3, pady=10)
        
        # File list display
        files_frame = ttk.LabelFrame(main_frame, text="Files to Convert", padding="10")
        files_frame.grid(row=3, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        files_frame.columnconfigure(0, weight=1)
        files_frame.rowconfigure(0, weight=1)
        
        # Files listbox
        listbox_frame = ttk.Frame(files_frame)
        listbox_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        listbox_frame.columnconfigure(0, weight=1)
        listbox_frame.rowconfigure(0, weight=1)
        
        self.files_listbox = tk.Listbox(listbox_frame, height=10)
        self.files_listbox.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Scrollbar for listbox
        scrollbar = ttk.Scrollbar(listbox_frame, orient=tk.VERTICAL, command=self.files_listbox.yview)
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        self.files_listbox.configure(yscrollcommand=scrollbar.set)
        
        # Progress section
        progress_frame = ttk.Frame(main_frame)
        progress_frame.grid(row=4, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10))
        progress_frame.columnconfigure(0, weight=1)
        
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(progress_frame, variable=self.progress_var, 
                                          maximum=100, length=400)
        self.progress_bar.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 5))
        
        self.status_var = tk.StringVar(value="Ready - Select input and output folders")
        self.status_label = ttk.Label(progress_frame, textvariable=self.status_var)
        self.status_label.grid(row=1, column=0, sticky=tk.W)
        
        # Convert button
        self.convert_button = ttk.Button(main_frame, text="Convert All Files", 
                                       command=self.start_conversion, 
                                       style='Accent.TButton')
        self.convert_button.grid(row=5, column=0, columnspan=3, pady=20)
        self.convert_button.config(state='disabled')
        
        # Configure accent style for the button
        self.style.configure('Accent.TButton', font=('Arial', 12, 'bold'))
    
    def select_input_folder(self):
        """Select input folder"""
        folder_path = filedialog.askdirectory(title="Select Input Folder")
        if folder_path:
            self.input_folder = folder_path
            self.input_var.set(folder_path)
            self.update_status("Input folder selected. Click 'Scan Input Folder' to find audio files.")
    
    def select_output_folder(self):
        """Select output folder"""
        folder_path = filedialog.askdirectory(title="Select Output Folder")
        if folder_path:
            self.output_folder = folder_path
            self.output_var.set(folder_path)
            self.update_status("Output folder selected.")
            self.check_ready_to_process()
    
    def scan_input_folder(self):
        """Scan input folder for audio files"""
        if not self.input_folder:
            messagebox.showwarning("No Input Folder", "Please select an input folder first.")
            return
        
        self.update_status("Scanning input folder for audio files...")
        self.audio_files.clear()
        self.files_listbox.delete(0, tk.END)
        
        # Supported audio formats
        audio_extensions = ('.mp3', '.wav', '.flac', '.m4a', '.aac', '.ogg', '.wma', '.opus')
        
        # Scan for audio files
        for root, dirs, files in os.walk(self.input_folder):
            for file in files:
                if file.lower().endswith(audio_extensions):
                    file_path = os.path.join(root, file)
                    self.audio_files.append(file_path)
                    # Display relative path in listbox
                    rel_path = os.path.relpath(file_path, self.input_folder)
                    self.files_listbox.insert(tk.END, rel_path)
        
        file_count = len(self.audio_files)
        if file_count > 0:
            self.update_status(f"Found {file_count} audio file(s) to convert.")
        else:
            self.update_status("No audio files found in the input folder.")
            messagebox.showinfo("No Files Found", 
                              "No audio files were found in the selected input folder.\n\n"
                              f"Supported formats: {', '.join(audio_extensions)}")
        
        self.check_ready_to_process()
    
    def check_ready_to_process(self):
        """Check if we're ready to process files"""
        if self.input_folder and self.output_folder and len(self.audio_files) > 0:
            self.convert_button.config(state='normal')
        else:
            self.convert_button.config(state='disabled')
    
    def start_conversion(self):
        """Start the conversion in a separate thread"""
        if not self.audio_files:
            messagebox.showwarning("No Files", "Please scan the input folder first.")
            return
        
        if not self.output_folder:
            messagebox.showwarning("No Output Folder", "Please select an output folder.")
            return
        
        # Disable the convert button during processing
        self.convert_button.config(state='disabled')
        
        # Start conversion in a separate thread
        thread = threading.Thread(target=self.convert_files)
        thread.daemon = True
        thread.start()
    
    def convert_files(self):
        """Convert all audio files to MP3 64kbps"""
        try:
            # Create output folder if it doesn't exist
            os.makedirs(self.output_folder, exist_ok=True)
            
            self.update_status("Starting conversion...")
            self.update_progress(0)
            
            total_files = len(self.audio_files)
            converted_files = 0
            failed_files = []
            
            for i, input_path in enumerate(self.audio_files):
                # Get the original filename without extension
                filename = os.path.basename(input_path)
                name_without_ext = os.path.splitext(filename)[0]
                
                # Create output path with .mp3 extension
                output_filename = f"{name_without_ext}.mp3"
                output_path = os.path.join(self.output_folder, output_filename)
                
                self.update_status(f"Converting {i + 1}/{total_files}: {filename}")
                
                try:
                    # Load the audio file
                    audio = AudioSegment.from_file(input_path)
                    
                    # Export as MP3 with 64kbps bitrate
                    audio.export(
                        output_path,
                        format="mp3",
                        bitrate="64k"
                    )
                    
                    converted_files += 1
                    
                    # Update listbox to show file is processed
                    rel_path = os.path.relpath(input_path, self.input_folder)
                    for idx in range(self.files_listbox.size()):
                        if self.files_listbox.get(idx) == rel_path:
                            self.files_listbox.itemconfig(idx, {'fg': 'green'})
                            break
                    
                except Exception as e:
                    failed_files.append(f"{filename}: {str(e)}")
                    # Mark failed file in red
                    rel_path = os.path.relpath(input_path, self.input_folder)
                    for idx in range(self.files_listbox.size()):
                        if self.files_listbox.get(idx) == rel_path:
                            self.files_listbox.itemconfig(idx, {'fg': 'red'})
                            break
                
                # Update progress
                progress = ((i + 1) / total_files) * 100
                self.update_progress(progress)
            
            # Show completion message
            if failed_files:
                error_message = "\n".join(failed_files[:10])  # Show first 10 errors
                if len(failed_files) > 10:
                    error_message += f"\n... and {len(failed_files) - 10} more errors"
                
                messagebox.showwarning("Conversion Complete with Errors", 
                                     f"Conversion completed!\n\n"
                                     f"Successfully converted: {converted_files}/{total_files}\n"
                                     f"Failed: {len(failed_files)}\n\n"
                                     f"Errors:\n{error_message}")
            else:
                messagebox.showinfo("Conversion Complete", 
                                  f"Successfully converted all {converted_files} file(s) to MP3 64kbps!\n\n"
                                  f"Output folder: {self.output_folder}")
            
            self.update_status(f"Conversion completed. {converted_files}/{total_files} files converted successfully.")
            
        except Exception as e:
            self.update_status(f"Conversion error: {str(e)}")
            messagebox.showerror("Error", f"An error occurred during conversion:\n\n{str(e)}")
        
        finally:
            # Re-enable the convert button
            self.convert_button.config(state='normal')
            self.update_progress(0)
    
    def update_status(self, message):
        """Update status label"""
        self.status_var.set(message)
        self.root.update_idletasks()
    
    def update_progress(self, value):
        """Update progress bar"""
        self.progress_var.set(value)
        self.root.update_idletasks()

def find_ffmpeg():
    """Try to find ffmpeg in various locations"""
    # Try standard which first
    ffmpeg_path = which("ffmpeg")
    if ffmpeg_path:
        return ffmpeg_path
    
    # Try common Windows locations
    common_paths = [
        r"C:\ffmpeg\bin\ffmpeg.exe",
        r"C:\Program Files\ffmpeg\bin\ffmpeg.exe",
        r"C:\Program Files (x86)\ffmpeg\bin\ffmpeg.exe",
        r"C:\ProgramData\chocolatey\lib\ffmpeg\tools\ffmpeg\bin\ffmpeg.exe",
        r"C:\ProgramData\chocolatey\bin\ffmpeg.exe"
    ]
    
    for path in common_paths:
        if os.path.exists(path):
            return path
    
    return None

def main():
    # Check if ffmpeg is available
    ffmpeg_path = find_ffmpeg()
    if not ffmpeg_path:
        root = tk.Tk()
        root.withdraw()  # Hide the main window
        messagebox.showerror("Missing Dependency", 
                           "FFmpeg is required for audio processing.\n\n"
                           "Please install FFmpeg and make sure it's in your system PATH.\n"
                           "You can download it from: https://ffmpeg.org/download.html")
        return
    
    # Set the ffmpeg path for pydub if we found it in a non-standard location
    if ffmpeg_path != which("ffmpeg"):
        # Set environment variables for FFmpeg tools
        ffmpeg_dir = os.path.dirname(ffmpeg_path)
        os.environ["PATH"] = ffmpeg_dir + os.pathsep + os.environ.get("PATH", "")
        
        # Also set pydub attributes directly
        AudioSegment.converter = ffmpeg_path
        AudioSegment.ffmpeg = ffmpeg_path
        AudioSegment.ffprobe = ffmpeg_path.replace("ffmpeg.exe", "ffprobe.exe")
        AudioSegment.ffplay = ffmpeg_path.replace("ffmpeg.exe", "ffplay.exe")
    
    root = tk.Tk()
    app = AudioTypeChangerApp(root)
    
    # Center the window
    root.update_idletasks()
    x = (root.winfo_screenwidth() // 2) - (root.winfo_width() // 2)
    y = (root.winfo_screenheight() // 2) - (root.winfo_height() // 2)
    root.geometry(f"+{x}+{y}")
    
    root.mainloop()

if __name__ == "__main__":
    main()
