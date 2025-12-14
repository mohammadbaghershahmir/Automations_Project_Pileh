import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
from pathlib import Path
import threading
import subprocess
import re
from collections import defaultdict
from pydub import AudioSegment
from pydub.utils import which

class AudioConcatenatorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Audio File Batch Processor")
        self.root.geometry("800x650")
        self.root.resizable(True, True)
        
        # Configure style
        self.style = ttk.Style()
        self.style.theme_use('clam')
        
        # Folder paths
        self.input_folder = ""
        self.output_folder = ""
        
        # File groups for processing
        self.file_groups = {}
        
        self.setup_ui()
        self.check_intro_outro_files()
        
    def setup_ui(self):
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(2, weight=1)
        
        # Title
        title_label = ttk.Label(main_frame, text="Audio File Batch Processor", 
                               font=('Arial', 16, 'bold'))
        title_label.grid(row=0, column=0, columnspan=3, pady=(0, 20))
        
        # Description
        desc_label = ttk.Label(main_frame, 
                              text="Processes audio files with pattern: [a|b]_xxxxxx_xofx\n" +
                                   "Groups files by prefix and 6-digit number, adds intro/outro",
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
        
        # File groups display
        groups_frame = ttk.LabelFrame(main_frame, text="Detected File Groups", padding="10")
        groups_frame.grid(row=3, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        groups_frame.columnconfigure(0, weight=1)
        groups_frame.rowconfigure(0, weight=1)
        
        # Groups treeview
        self.groups_tree = ttk.Treeview(groups_frame, columns=('files', 'status'), show='tree headings', height=8)
        self.groups_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure columns
        self.groups_tree.heading('#0', text='Group')
        self.groups_tree.heading('files', text='Files Count')
        self.groups_tree.heading('status', text='Status')
        
        self.groups_tree.column('#0', width=200)
        self.groups_tree.column('files', width=100)
        self.groups_tree.column('status', width=150)
        
        # Scrollbar for treeview
        tree_scrollbar = ttk.Scrollbar(groups_frame, orient=tk.VERTICAL, command=self.groups_tree.yview)
        tree_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        self.groups_tree.configure(yscrollcommand=tree_scrollbar.set)
        
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
        
        # Process button
        self.process_button = ttk.Button(main_frame, text="Process All Groups", 
                                       command=self.start_processing, 
                                       style='Accent.TButton')
        self.process_button.grid(row=5, column=0, columnspan=3, pady=20)
        self.process_button.config(state='disabled')
        
        # Configure accent style for the button
        self.style.configure('Accent.TButton', font=('Arial', 12, 'bold'))
    
    def check_intro_outro_files(self):
        """Check for intro/outro files in the main application folder at startup"""
        app_folder = os.getcwd()
        intro_outro_names = ['a_int.mp3', 'b_int.mp3', 'a_out.mp3', 'b_out.mp3']
        found_files = []
        
        for filename in intro_outro_names:
            file_path = os.path.join(app_folder, filename)
            if os.path.exists(file_path):
                found_files.append(filename)
        
        if found_files:
            status_msg = f"Found intro/outro files: {', '.join(found_files)}"
        else:
            status_msg = "No intro/outro files found in main folder. Place a_int.mp3, b_int.mp3, a_out.mp3, b_out.mp3 here if needed."
        
        self.update_status(status_msg)
        
    def select_input_folder(self):
        """Select input folder"""
        folder_path = filedialog.askdirectory(title="Select Input Folder")
        if folder_path:
            self.input_folder = folder_path
            self.input_var.set(folder_path)
            self.update_status("Input folder selected. Click 'Scan Input Folder' to analyze files.")
    
    def select_output_folder(self):
        """Select output folder"""
        folder_path = filedialog.askdirectory(title="Select Output Folder")
        if folder_path:
            self.output_folder = folder_path
            self.output_var.set(folder_path)
            self.update_status("Output folder selected.")
            self.check_ready_to_process()
    
    def scan_input_folder(self):
        """Scan input folder for audio files and group them"""
        if not self.input_folder:
            messagebox.showwarning("No Input Folder", "Please select an input folder first.")
            return
        
        self.update_status("Scanning input folder and checking for intro/outro files...")
        self.file_groups.clear()
        
        # Clear the treeview
        for item in self.groups_tree.get_children():
            self.groups_tree.delete(item)
        
        # Pattern to match files: [a|b]_xxxxxx_xofx
        pattern = re.compile(r'^([ab])_(\d{6})_(\d+)of(\d+)\.(wav|mp3)$', re.IGNORECASE)
        
        # First, look for intro/outro files in the main application folder
        app_folder = os.getcwd()  # Current working directory where the app is running
        intro_outro_files = {}
        
        intro_outro_names = ['a_int.mp3', 'b_int.mp3', 'a_out.mp3', 'b_out.mp3']
        for filename in intro_outro_names:
            file_path = os.path.join(app_folder, filename)
            if os.path.exists(file_path):
                intro_outro_files[filename.lower()] = file_path
        
        # Scan input folder for main audio files (excluding intro/outro)
        audio_files = []
        for root, dirs, files in os.walk(self.input_folder):
            for file in files:
                if file.lower().endswith(('.wav', '.mp3')):
                    # Skip intro/outro files if found in input folder
                    if file.lower() not in intro_outro_names:
                        audio_files.append(os.path.join(root, file))
        
        # Group files by pattern
        groups = defaultdict(list)
        
        for file_path in audio_files:
            filename = os.path.basename(file_path)
            
            # Check if file matches the pattern
            match = pattern.match(filename)
            if match:
                prefix, number, part, total, ext = match.groups()
                group_key = f"{prefix}_{number}"
                groups[group_key].append({
                    'path': file_path,
                    'part': int(part),
                    'total': int(total),
                    'extension': ext
                })
        
        # Process and validate groups
        valid_groups = {}
        for group_key, files in groups.items():
            prefix = group_key[0]  # 'a' or 'b'
            
            # Sort files by part number
            files.sort(key=lambda x: x['part'])
            
            # Check if we have all parts
            if files:
                expected_total = files[0]['total']
                actual_parts = [f['part'] for f in files]
                expected_parts = list(range(1, expected_total + 1))
                
                if actual_parts == expected_parts:
                    # Check for intro/outro files
                    intro_file = intro_outro_files.get(f'{prefix}_int.mp3')
                    outro_file = intro_outro_files.get(f'{prefix}_out.mp3')
                    
                    valid_groups[group_key] = {
                        'files': files,
                        'intro': intro_file,
                        'outro': outro_file,
                        'prefix': prefix,
                        'complete': True
                    }
                else:
                    valid_groups[group_key] = {
                        'files': files,
                        'intro': intro_outro_files.get(f'{prefix}_int.mp3'),
                        'outro': intro_outro_files.get(f'{prefix}_out.mp3'),
                        'prefix': prefix,
                        'complete': False,
                        'missing_parts': set(expected_parts) - set(actual_parts)
                    }
        
        self.file_groups = valid_groups
        
        # Update the treeview
        for group_key, group_data in valid_groups.items():
            files_count = len(group_data['files'])
            intro_outro_count = sum([1 for x in [group_data['intro'], group_data['outro']] if x])
            total_files = files_count + intro_outro_count
            
            if group_data['complete']:
                status = f"Complete ({total_files} files)"
                if not group_data['intro']:
                    status += " - Missing intro"
                if not group_data['outro']:
                    status += " - Missing outro"
            else:
                missing = ', '.join(map(str, group_data['missing_parts']))
                status = f"Incomplete - Missing parts: {missing}"
            
            item = self.groups_tree.insert('', 'end', text=group_key, 
                                         values=(total_files, status))
            
            # Add file details as children
            if group_data['intro']:
                self.groups_tree.insert(item, 'end', text=f"  └ {os.path.basename(group_data['intro'])}", 
                                      values=('', 'Intro'))
            
            for file_data in group_data['files']:
                filename = os.path.basename(file_data['path'])
                self.groups_tree.insert(item, 'end', text=f"  └ {filename}", 
                                      values=('', f"Part {file_data['part']}/{file_data['total']}"))
            
            if group_data['outro']:
                self.groups_tree.insert(item, 'end', text=f"  └ {os.path.basename(group_data['outro'])}", 
                                      values=('', 'Outro'))
        
        total_groups = len(valid_groups)
        complete_groups = sum(1 for g in valid_groups.values() if g['complete'])
        
        # Create a detailed status message
        intro_outro_list = list(intro_outro_files.keys())
        if intro_outro_list:
            intro_outro_msg = f"Available intro/outro: {', '.join(intro_outro_list)}"
        else:
            intro_outro_msg = "No intro/outro files found in main folder"
        
        self.update_status(f"Found {total_groups} groups ({complete_groups} complete). {intro_outro_msg}")
        
        self.check_ready_to_process()
    
    def check_ready_to_process(self):
        """Check if we're ready to process files"""
        if self.input_folder and self.output_folder and self.file_groups:
            complete_groups = sum(1 for g in self.file_groups.values() if g['complete'])
            if complete_groups > 0:
                self.process_button.config(state='normal')
                return
        
        self.process_button.config(state='disabled')
    
    def start_processing(self):
        """Start the batch processing in a separate thread"""
        if not self.file_groups:
            messagebox.showwarning("No Groups", "Please scan the input folder first.")
            return
        
        if not self.output_folder:
            messagebox.showwarning("No Output Folder", "Please select an output folder.")
            return
        
        # Check if there are complete groups to process
        complete_groups = [k for k, v in self.file_groups.items() if v['complete']]
        if not complete_groups:
            messagebox.showwarning("No Complete Groups", "No complete file groups found to process.")
            return
        
        # Disable the process button during processing
        self.process_button.config(state='disabled')
        
        # Start processing in a separate thread
        thread = threading.Thread(target=self.process_file_groups)
        thread.daemon = True
        thread.start()
    
    def process_file_groups(self):
        """Process all complete file groups"""
        try:
            self.update_status("Starting batch processing...")
            self.update_progress(0)
            
            complete_groups = [(k, v) for k, v in self.file_groups.items() if v['complete']]
            total_groups = len(complete_groups)
            processed_groups = 0
            
            for i, (group_key, group_data) in enumerate(complete_groups):
                self.update_status(f"Processing group {i + 1} of {total_groups}: {group_key}")
                
                try:
                    # Process this group
                    output_filename = f"{group_key}.mp3"
                    output_path = os.path.join(self.output_folder, output_filename)
                    
                    success = self.process_single_group(group_key, group_data, output_path)
                    
                    if success:
                        processed_groups += 1
                        # Update the treeview status
                        for item in self.groups_tree.get_children():
                            if self.groups_tree.item(item, 'text') == group_key:
                                current_values = list(self.groups_tree.item(item, 'values'))
                                current_values[1] = "✓ Processed"
                                self.groups_tree.item(item, values=current_values)
                                break
                    
                except Exception as e:
                    self.update_status(f"Error processing group {group_key}: {str(e)}")
                    # Update the treeview status
                    for item in self.groups_tree.get_children():
                        if self.groups_tree.item(item, 'text') == group_key:
                            current_values = list(self.groups_tree.item(item, 'values'))
                            current_values[1] = f"✗ Error: {str(e)[:30]}..."
                            self.groups_tree.item(item, values=current_values)
                            break
                
                # Update progress
                progress = ((i + 1) / total_groups) * 100
                self.update_progress(progress)
            
            self.update_status(f"Batch processing completed. Processed {processed_groups} of {total_groups} groups.")
            
            # Show completion message
            messagebox.showinfo("Batch Processing Complete", 
                              f"Processing completed!\n\n"
                              f"Groups processed: {processed_groups}/{total_groups}\n"
                              f"Output folder: {self.output_folder}")
            
        except Exception as e:
            self.update_status(f"Batch processing error: {str(e)}")
            messagebox.showerror("Error", f"An error occurred during batch processing:\n\n{str(e)}")
        
        finally:
            # Re-enable the process button
            self.process_button.config(state='normal')
            self.update_progress(0)
    
    def process_single_group(self, group_key, group_data, output_path):
        """Process a single file group"""
        try:
            combined_audio = None
            
            # Add intro if available
            if group_data['intro']:
                self.update_status(f"Adding intro for {group_key}...")
                intro_audio = AudioSegment.from_file(group_data['intro'])
                # Standardize audio properties
                if intro_audio.frame_rate != 44100:
                    intro_audio = intro_audio.set_frame_rate(44100)
                if intro_audio.channels != 2:
                    intro_audio = intro_audio.set_channels(2)
                combined_audio = intro_audio
            
            # Add main files in order
            for file_data in group_data['files']:
                self.update_status(f"Processing {group_key} part {file_data['part']}/{file_data['total']}...")
                
                audio = AudioSegment.from_file(file_data['path'])
                
                # Standardize audio properties
                if audio.frame_rate != 44100:
                    audio = audio.set_frame_rate(44100)
                if audio.channels != 2:
                    audio = audio.set_channels(2)
                
                if combined_audio is None:
                    combined_audio = audio
                else:
                    combined_audio += audio
            
            # Add outro if available
            if group_data['outro']:
                self.update_status(f"Adding outro for {group_key}...")
                outro_audio = AudioSegment.from_file(group_data['outro'])
                # Standardize audio properties
                if outro_audio.frame_rate != 44100:
                    outro_audio = outro_audio.set_frame_rate(44100)
                if outro_audio.channels != 2:
                    outro_audio = outro_audio.set_channels(2)
                combined_audio += outro_audio
            
            if combined_audio is None:
                raise Exception("No audio content to process")
            
            # Export as MP3 with 64kbps quality
            self.update_status(f"Exporting {group_key}...")
            combined_audio.export(
                output_path,
                format="mp3",
                bitrate="64k",
                tags={"title": group_key, "artist": "Audio Batch Processor"}
            )
            
            return True
            
        except Exception as e:
            raise Exception(f"Failed to process {group_key}: {str(e)}")
    
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
                           "You can download it from: https://ffmpeg.org/download.html\n\n"
                           "Run 'python check_ffmpeg.py' for detailed diagnostics.")
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
    app = AudioConcatenatorApp(root)
    
    # Center the window
    root.update_idletasks()
    x = (root.winfo_screenwidth() // 2) - (root.winfo_width() // 2)
    y = (root.winfo_screenheight() // 2) - (root.winfo_height() // 2)
    root.geometry(f"+{x}+{y}")
    
    root.mainloop()

if __name__ == "__main__":
    main()
