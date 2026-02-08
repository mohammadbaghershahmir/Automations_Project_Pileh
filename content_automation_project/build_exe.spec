# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec for Content Automation Project - Windows executable
# Run from content_automation_project directory:
#   pyinstaller build_exe.spec --clean --noconfirm

import sys

block_cipher = None

# All project modules (so they are included when run from project root)
project_scripts = ['run.py']
project_modules = [
    'main_gui', 'api_layer', 'prompt_manager', 'pdf_processor', 'word_file_processor',
    'multi_part_post_processor', 'multi_part_processor', 'third_stage_converter',
    'third_stage_chunk_processor', 'base_stage_processor', 'unified_api_client',
    'deepseek_api_client', 'pre_ocr_topic_processor', 'stage_e_processor',
    'stage_f_processor', 'stage_h_processor', 'stage_j_processor', 'stage_l_processor',
    'stage_m_processor', 'stage_v_processor', 'stage_x_processor', 'stage_y_processor',
    'stage_z_processor', 'reference_change_rag', 'automated_pipeline_orchestrator',
    'txt_stage_json_utils',
]

# Data files to bundle (prompts.json must be next to exe at runtime)
added_data = [('prompts.json', '.')]

a = Analysis(
    project_scripts,
    pathex=[],
    binaries=[],
    datas=added_data,
    hiddenimports=[
        'PIL', 'PIL._tkinter_finder',
        'docx', 'python-docx',
        'fitz',  # PyMuPDF
        'google.generativeai', 'google.genai',
        'requests', 'customtkinter',
        'sentence_transformers',  # optional RAG
        'numpy',
    ] + project_modules,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'matplotlib', 'scipy', 'pandas', 'tkinter.test',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='ContentAutomation',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,   # No console window (GUI app)
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
