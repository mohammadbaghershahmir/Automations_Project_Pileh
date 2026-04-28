#!/usr/bin/env python3
"""
Test script for DeepSeek API connectivity and response
"""

import sys
import os
import json
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from deepseek_api_client import DeepSeekAPIClient
from api_layer import APIKeyManager

def test_deepseek_api(api_key: str):
    """Test DeepSeek API with a simple request"""
    
    print("=" * 60)
    print("Testing DeepSeek API Connection")
    print("=" * 60)
    
    # Create API key manager and add the key
    key_manager = APIKeyManager()
    key_manager.api_keys = [{
        'account': 'test_account',
        'project': 'test_project',
        'api_key': api_key
    }]
    
    # Initialize DeepSeek client
    client = DeepSeekAPIClient(api_key_manager=key_manager)
    
    # Test models to try
    test_models = [
        "deepseek-reasoner",
        "deepseek-chat",
        "deepseek-chat-v3",
    ]
    
    # Simple test prompt
    test_prompt = "Hello! Please respond with 'API is working' if you can read this."
    
    print(f"\nTest prompt: {test_prompt}")
    print(f"\nTesting {len(test_models)} models...\n")
    
    results = {}
    
    for model_name in test_models:
        print(f"\n{'='*60}")
        print(f"Testing model: {model_name}")
        print(f"{'='*60}")
        
        try:
            # Initialize the model
            print(f"Initializing {model_name}...")
            if not client.initialize_text_client(model_name, api_key):
                print(f"❌ Failed to initialize {model_name}")
                results[model_name] = {
                    'status': 'failed',
                    'error': 'Initialization failed'
                }
                continue
            
            print(f"✓ Model {model_name} initialized")
            
            # Send a simple request
            print(f"Sending test request to {model_name}...")
            response = client.process_text(
                text=test_prompt,
                system_prompt=None,
                model_name=model_name,
                temperature=0.7,
                max_tokens=100,
                api_key=api_key
            )
            
            if response:
                print(f"✓ SUCCESS! Received response from {model_name}")
                print(f"\nResponse length: {len(response)} characters")
                print(f"\nResponse preview (first 200 chars):")
                print(f"{response[:200]}...")
                print(f"\nFull response:")
                print(f"{response}")
                
                results[model_name] = {
                    'status': 'success',
                    'response_length': len(response),
                    'response_preview': response[:200],
                    'full_response': response
                }
            else:
                print(f"❌ No response received from {model_name}")
                results[model_name] = {
                    'status': 'failed',
                    'error': 'No response received'
                }
                
        except Exception as e:
            print(f"❌ ERROR testing {model_name}: {str(e)}")
            import traceback
            traceback.print_exc()
            results[model_name] = {
                'status': 'error',
                'error': str(e)
            }
    
    # Summary
    print(f"\n\n{'='*60}")
    print("TEST SUMMARY")
    print(f"{'='*60}")
    
    success_count = sum(1 for r in results.values() if r['status'] == 'success')
    total_count = len(results)
    
    print(f"\nTotal models tested: {total_count}")
    print(f"Successful: {success_count}")
    print(f"Failed: {total_count - success_count}")
    
    print(f"\nDetailed results:")
    for model_name, result in results.items():
        status_icon = "✓" if result['status'] == 'success' else "❌"
        print(f"\n{status_icon} {model_name}: {result['status']}")
        if result['status'] == 'success':
            print(f"   Response length: {result['response_length']} chars")
        else:
            print(f"   Error: {result.get('error', 'Unknown error')}")
    
    # Save results to JSON file
    output_file = "deepseek_api_test_results.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n✓ Results saved to: {output_file}")
    
    return results

if __name__ == "__main__":
    # Get API key from command line or use provided one
    if len(sys.argv) > 1:
        api_key = sys.argv[1]
    else:
        # Use the provided API key
        api_key = "sk-3495c7b61f474f4699d476467085fad0"
    
    print(f"Using API key: {api_key[:10]}...{api_key[-4:]}")
    
    results = test_deepseek_api(api_key)
    
    # Exit with appropriate code
    success_count = sum(1 for r in results.values() if r['status'] == 'success')
    sys.exit(0 if success_count > 0 else 1)

