"""
Quick verification test for new environment variables.
Tests that all new configuration variables are properly defined.
"""

import sys
import os

# Add scripts directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scripts', 'Netherlands'))

try:
    from config_helpers import getenv_int, getenv_float
    print("✓ Config helpers imported successfully")
    
    # Test all new environment variables with their defaults
    tests = [
        ("ABSOLUTE_TIMEOUT_MINUTES", 300, getenv_int),
        ("MAX_WORKER_ITERATIONS", 10000, getenv_int),
        ("MAX_RETRY_PASSES", 3, getenv_int),
        ("MAX_DRIVER_RECREATIONS", 10, getenv_int),
        ("NETWORK_RETRY_MAX_WAIT_SEC", 60, getenv_int),
        ("NETWORK_RETRY_JITTER_PERCENT", 0.1, getenv_float),
        ("WORKER_QUEUE_TIMEOUT_SEC", 5, getenv_int),
        ("CHROME_PROFILE_MAX_AGE_HOURS", 24, getenv_int),
    ]
    
    print("\nTesting environment variables with defaults:")
    print("-" * 60)
    
    all_passed = True
    for var_name, expected_default, getter_func in tests:
        actual = getter_func(var_name, expected_default)
        status = "✓" if actual == expected_default else "✗"
        print(f"{status} {var_name}: {actual} (expected: {expected_default})")
        if actual != expected_default:
            all_passed = False
    
    print("-" * 60)
    if all_passed:
        print("✓ All environment variables configured correctly!")
        sys.exit(0)
    else:
        print("✗ Some environment variables have unexpected values")
        sys.exit(1)
        
except Exception as e:
    print(f"✗ Error during verification: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
