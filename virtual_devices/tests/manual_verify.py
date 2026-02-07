"""Manual verification script for simulators"""

import json
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from virtual_devices.simulators.wearable import WearableSimulator
from virtual_devices.simulators.restaurant import RestaurantSimulator
from virtual_devices.simulators.gps import GPSSimulator



def test_basic_generation():
    """Test from Step 129: Basic generation of normal/bad data"""
    print("\n" + "="*40)
    print("TEST: Basic Generation (Single Sample)")
    print("="*40)
    
    print('Testing Wearable Simulator...')
    ws = WearableSimulator()
    data = ws.generate_normal_data()
    print('Normal:', json.dumps(data))
    data = ws.generate_bad_data()
    print('Bad:', json.dumps(data))

    print('\nTesting Restaurant Simulator...')
    rs = RestaurantSimulator()
    data = rs.generate_normal_data()
    print('Normal:', json.dumps(data)[:300])

    print('\nTesting GPS Simulator...')
    gps = GPSSimulator(city='bangalore')
    data = gps.generate_normal_data()
    print('Normal:', json.dumps(data))
    print('All basic tests passed!')


def test_iterative_generation():
    """Test from Step 109: Generating multiple samples to check variance"""
    print("\n" + "="*40)
    print("TEST: Iterative Generation (3 Samples)")
    print("="*40)

    print('=== Testing Wearable Simulator ===')
    ws = WearableSimulator(device_id='test_wearable_001', bad_data_probability=0.2)
    for i in range(3):
        data = ws.generate_data()
        print(f'Wearable [{i+1}]: {json.dumps(data, indent=2)[:200]}...')

    print('\n=== Testing Restaurant Simulator ===')
    rs = RestaurantSimulator(device_id='test_restaurant_001', bad_data_probability=0.2)
    for i in range(3):
        data = rs.generate_data()
        print(f'Restaurant [{i+1}]: {json.dumps(data, indent=2)[:200]}...')

    print('\n=== Testing GPS Simulator ===')
    gps = GPSSimulator(device_id='test_gps_001', city='bangalore', bad_data_probability=0.2)
    for i in range(3):
        data = gps.generate_data()
        print(f'GPS [{i+1}]: {json.dumps(data, indent=2)[:200]}...')


def test_bad_data_generation():
    """Test from Step 166: Explicit Bad Data Verification"""
    print("\n" + "="*40)
    print("TEST: Bad Data Generation")
    print("="*40)

    print('=== Restaurant Simulator ===')
    rs = RestaurantSimulator()
    print('Normal:', json.dumps(rs.generate_normal_data(), indent=2))

    print('\n=== GPS Simulator (Bangalore) ===')
    gps = GPSSimulator(city='bangalore')
    print('Normal:', json.dumps(gps.generate_normal_data(), indent=2))

    print('\n=== Bad Data Examples ===')
    
    from virtual_devices.simulators.wearable import WearableSimulator
    ws = WearableSimulator()
    print('Bad Wearable:', json.dumps(ws.generate_bad_data(), indent=2))
    print('Bad GPS:', json.dumps(gps.generate_bad_data(), indent=2))
    print('All bad data tests passed!')


def run_all_tests():
    """Run all consolidated tests"""
    print("Running consolidated verification tests...")
    try:
        test_basic_generation()
        test_iterative_generation()
        test_bad_data_generation()
        print("\n\n✅ ALL VALIDATION TESTS PASSED")
        return True
    except Exception as e:
        print(f"\n❌ TESTS FAILED: {e}")
        return False

if __name__ == "__main__":
    run_all_tests()
