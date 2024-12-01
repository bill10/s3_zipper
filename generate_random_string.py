#!/usr/bin/env python3

import argparse
import random
import string

def generate_random_string(length=8):
    """Generate a random string of specified length using letters and numbers.
    
    Args:
        length (int): Length of the random string to generate (default: 8)
        
    Returns:
        str: Random string of specified length
    """
    # Use both letters and numbers for the random string
    characters = string.ascii_letters + string.digits
    return ''.join(random.choices(characters, k=length))

def main():
    parser = argparse.ArgumentParser(description='Generate a random string of specified length')
    parser.add_argument(
        '-l', '--length',
        type=int,
        default=8,
        help='Length of the random string (default: 8)'
    )
    args = parser.parse_args()
    
    if args.length < 1:
        print("Error: Length must be a positive integer")
        return 1
    
    # Generate and print the random string
    random_string = generate_random_string(args.length)
    print(random_string)
    return 0

if __name__ == '__main__':
    exit(main())
