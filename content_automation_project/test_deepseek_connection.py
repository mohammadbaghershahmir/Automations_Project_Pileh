#!/usr/bin/env python3
"""
Test script to check if DeepSeek API connection issues are from server or client side
"""

import sys
import os
import json
import time
import requests
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

def test_deepseek_connection(api_key: str, payload_size_kb: float = 46.0):
    """Test DeepSeek API with different payload sizes"""
    
    print("=" * 70)
    print("Testing DeepSeek API Connection Stability")
    print("=" * 70)
    
    base_url = "https://api.deepseek.com/v1/chat/completions"
    
    # Test models
    test_models = ["deepseek-reasoner", "deepseek-chat"]
    
    # Create test payloads of different sizes
    # Base message
    base_text = "Hello! Please respond with a detailed explanation."
    
    # Create large payload (similar to your 46 KB payload)
    # We'll create a JSON-like structure to simulate the actual payload
    large_text = base_text
    target_size_bytes = int(payload_size_kb * 1024)
    
    # Add content to reach target size
    while len(large_text.encode('utf-8')) < target_size_bytes - 2000:  # Leave room for JSON structure
        large_text += "\n" + json.dumps({
            "topic": "test_topic",
            "content": "This is test content to simulate a large payload. " * 10,
            "extractions": ["extraction1", "extraction2", "extraction3"] * 5
        }, ensure_ascii=False)
    
    # Ensure we're close to target size
    current_size = len(large_text.encode('utf-8'))
    if current_size < target_size_bytes:
        padding = "x" * (target_size_bytes - current_size - 1000)
        large_text += padding
    
    test_payloads = [
        ("Small (1 KB)", base_text),
        ("Medium (10 KB)", large_text[:10*1024]),
        ("Large (46 KB)", large_text[:int(46*1024)]),
        ("Very Large (60 KB)", large_text[:int(60*1024)]),
    ]
    
    results = {}
    
    for model_name in test_models:
        print(f"\n{'='*70}")
        print(f"Testing Model: {model_name}")
        print(f"{'='*70}")
        
        results[model_name] = {}
        
        for payload_name, payload_text in test_payloads:
            print(f"\n--- Testing {payload_name} payload ---")
            
            payload_size = len(payload_text.encode('utf-8'))
            print(f"Payload size: {payload_size:,} bytes ({payload_size/1024:.2f} KB)")
            
            messages = [
                {"role": "user", "content": payload_text}
            ]
            
            request_payload = {
                "model": model_name,
                "messages": messages,
                "temperature": 0.7,
                "max_tokens": 4096,  # Reduced max_tokens like in your code
                "stream": False
            }
            
            # Calculate actual request size
            request_size = len(json.dumps(request_payload).encode('utf-8'))
            print(f"Request size: {request_size:,} bytes ({request_size/1024:.2f} KB)")
            
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
                "Connection": "keep-alive",
                "Keep-Alive": "timeout=600"
            }
            
            # Test with different timeouts
            timeouts_to_test = [
                (10, 300),   # 5 minutes
                (10, 600),   # 10 minutes
                (10, 1200),  # 20 minutes
            ]
            
            success = False
            error_type = None
            error_message = None
            response_time = None
            
            for connect_timeout, read_timeout in timeouts_to_test:
                print(f"  Trying with timeout: connect={connect_timeout}s, read={read_timeout}s...")
                
                try:
                    start_time = time.time()
                    
                    # Create a fresh session for each test
                    session = requests.Session()
                    session.headers.update(headers)
                    
                    response = session.post(
                        base_url,
                        json=request_payload,
                        timeout=(connect_timeout, read_timeout),
                        stream=False
                    )
                    
                    response_time = time.time() - start_time
                    
                    if response.status_code == 200:
                        result = response.json()
                        if "choices" in result and len(result["choices"]) > 0:
                            content = result["choices"][0]["message"]["content"]
                            print(f"  ✓ SUCCESS! Response received ({len(content)} chars) in {response_time:.2f}s")
                            success = True
                            error_type = None
                            break
                        else:
                            print(f"  ✗ Unexpected response format")
                            error_type = "unexpected_format"
                            error_message = "No choices in response"
                    else:
                        print(f"  ✗ HTTP {response.status_code}: {response.text[:200]}")
                        error_type = f"http_{response.status_code}"
                        error_message = response.text[:200]
                        
                except requests.exceptions.ChunkedEncodingError as e:
                    response_time = time.time() - start_time if 'start_time' in locals() else None
                    time_str = f"{response_time:.2f}s" if response_time else "unknown"
                    print(f"  ✗ ChunkedEncodingError after {time_str}: {str(e)}")
                    error_type = "ChunkedEncodingError"
                    error_message = str(e)
                    
                except requests.exceptions.Timeout as e:
                    print(f"  ✗ Timeout: {str(e)}")
                    error_type = "Timeout"
                    error_message = str(e)
                    
                except requests.exceptions.ConnectionError as e:
                    print(f"  ✗ ConnectionError: {str(e)}")
                    error_type = "ConnectionError"
                    error_message = str(e)
                    
                except Exception as e:
                    response_time = time.time() - start_time if 'start_time' in locals() else None
                    print(f"  ✗ Error: {type(e).__name__}: {str(e)}")
                    error_type = type(e).__name__
                    error_message = str(e)
                
                finally:
                    if 'session' in locals():
                        session.close()
            
            results[model_name][payload_name] = {
                'success': success,
                'error_type': error_type,
                'error_message': error_message,
                'response_time': response_time,
                'payload_size': payload_size,
                'request_size': request_size
            }
            
            if not success:
                print(f"  ❌ FAILED: {error_type}")
                if error_message:
                    print(f"     Message: {error_message[:100]}")
            else:
                print(f"  ✓ SUCCESS")
    
    # Summary
    print(f"\n\n{'='*70}")
    print("TEST SUMMARY")
    print(f"{'='*70}")
    
    for model_name, model_results in results.items():
        print(f"\n{model_name}:")
        for payload_name, result in model_results.items():
            status = "✓ SUCCESS" if result['success'] else f"✗ FAILED ({result['error_type']})"
            print(f"  {payload_name:20s}: {status}")
            if result['success'] and result['response_time']:
                print(f"    Response time: {result['response_time']:.2f}s")
            elif not result['success']:
                print(f"    Error: {result['error_type']}")
                if result['error_message']:
                    print(f"    Details: {result['error_message'][:100]}")
    
    # Determine if issue is server-side or client-side
    print(f"\n{'='*70}")
    print("ANALYSIS")
    print(f"{'='*70}")
    
    # Check if small payloads work but large ones fail
    all_models_fail_large = True
    any_model_succeeds_small = False
    
    for model_name, model_results in results.items():
        if 'Small' in model_results and model_results['Small']['success']:
            any_model_succeeds_small = True
        if 'Large' in model_results and model_results['Large']['success']:
            all_models_fail_large = False
    
    if any_model_succeeds_small and all_models_fail_large:
        print("\n⚠️  ISSUE APPEARS TO BE SERVER-SIDE:")
        print("   - Small payloads work fine")
        print("   - Large payloads (>40 KB) fail consistently")
        print("   - This suggests DeepSeek API has limitations on payload size")
        print("\n   RECOMMENDATIONS:")
        print("   1. Split large requests into smaller chunks")
        print("   2. Reduce payload size by removing unnecessary data")
        print("   3. Contact DeepSeek support about payload size limits")
    elif not any_model_succeeds_small:
        print("\n⚠️  ISSUE APPEARS TO BE CLIENT-SIDE OR NETWORK:")
        print("   - Even small payloads fail")
        print("   - Check API key, network connection, or firewall settings")
    else:
        print("\n✓  CONNECTION APPEARS STABLE:")
        print("   - Some payloads succeed")
        print("   - Issue may be intermittent or related to specific payload content")
    
    # Save results
    output_file = "deepseek_connection_test_results.json"
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
    
    # Test with 46 KB payload (similar to your actual payload)
    results = test_deepseek_connection(api_key, payload_size_kb=46.0)
    
    # Exit with appropriate code
    success_count = sum(
        1 for model_results in results.values()
        for result in model_results.values()
        if result['success']
    )
    sys.exit(0 if success_count > 0 else 1)

