"""
SIGNAL LOGIC COMPREHENSIVE AUDIT
Senior Quant Code Review - Line-by-line Function Analysis
===========================================================

Auditing signal_analyzer.py to identify why baseline win rate is 36%
"""

import ast
import inspect
from pathlib import Path

# Read the signal_analyzer.py file
signal_file = Path(r"g:\My Drive\3WinningOrderFlowModAgent2Relaxed3ThirdBot\signal_analyzer.py")

print("=" * 100)
print("SIGNAL LOGIC AUDIT - FUNCTION INVENTORY & ANALYSIS")
print("=" * 100)

# Parse AST to get function structure
with open(signal_file, 'r', encoding='utf-8', errors='replace') as f:
    content = f.read()
    try:
        tree = ast.parse(content)
    except SyntaxError as e:
        print(f"Syntax error in file: {e}")
        tree = None

# Extract functions and their info
functions = {}
if tree:
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            functions[node.name] = {
                'lineno': node.lineno,
                'args': [arg.arg for arg in node.args.args],
                'docstring': ast.get_docstring(node),
                'lines': len(node.body)
            }

print(f"\n✓ Total functions found: {len(functions)}")

# Categorize by purpose
signal_generation = ['generate_signal', 'analyze_signal', 'calculate_signal', 'build_signal', 'create_signal']
entry_exit = ['calculate_entry', 'calculate_exit', 'evaluate_entry', 'evaluate_exit', 'get_entry', 'get_exit']
setup_detection = ['detect_setup', 'identify_setup', 'classify_setup', 'setup', 'structure']
order_flow = ['order_flow', 'pressure', 'imbalance', 'cvd', 'microstructure']
validation = ['validate', 'evaluate_gate', 'check_gate', 'filter', 'quality_gate']
scoring = ['score', 'rate', 'rank', 'calculate_score']
quality = ['quality', 'health', 'audit']

categories = {
    'SIGNAL_GENERATION': [],
    'ENTRY_EXIT_LOGIC': [],
    'SETUP_DETECTION': [],
    'ORDER_FLOW_ANALYSIS': [],
    'VALIDATION_GATES': [],
    'SCORING_FUNCTIONS': [],
    'QUALITY_HEALTH': [],
    'OTHER': []
}

for func_name in sorted(functions.keys()):
    func_name_lower = func_name.lower()
    categorized = False

    for kw in signal_generation:
        if kw in func_name_lower:
            categories['SIGNAL_GENERATION'].append(func_name)
            categorized = True
            break
    if not categorized:
        for kw in entry_exit:
            if kw in func_name_lower:
                categories['ENTRY_EXIT_LOGIC'].append(func_name)
                categorized = True
                break
    if not categorized:
        for kw in setup_detection:
            if kw in func_name_lower:
                categories['SETUP_DETECTION'].append(func_name)
                categorized = True
                break
    if not categorized:
        for kw in order_flow:
            if kw in func_name_lower:
                categories['ORDER_FLOW_ANALYSIS'].append(func_name)
                categorized = True
                break
    if not categorized:
        for kw in validation:
            if kw in func_name_lower:
                categories['VALIDATION_GATES'].append(func_name)
                categorized = True
                break
    if not categorized:
        for kw in scoring:
            if kw in func_name_lower:
                categories['SCORING_FUNCTIONS'].append(func_name)
                categorized = True
                break
    if not categorized:
        for kw in quality:
            if kw in func_name_lower:
                categories['QUALITY_HEALTH'].append(func_name)
                categorized = True
                break

    if not categorized:
        categories['OTHER'].append(func_name)

# Print categorized functions
for category, funcs in categories.items():
    if funcs:
        print(f"\n[{category}] ({len(funcs)} functions)")
        print("-" * 100)
        for func in sorted(funcs)[:15]:  # Show top 15
            info = functions[func]
            print(f"  {func:50s} | Line {info['lineno']:5d} | Args: {len(info['args']):2d} | {info['docstring'][:40] if info['docstring'] else 'NO DOCSTRING'}")
        if len(funcs) > 15:
            print(f"  ... and {len(funcs) - 15} more")

# Now perform deep analysis on CRITICAL FUNCTIONS
print("\n" + "=" * 100)
print("CRITICAL FUNCTION DEEP ANALYSIS")
print("=" * 100)

# Find the main signal generation entry point
lines = content.split('\n')

# Look for async functions (entry points)
async_funcs = []
for i, line in enumerate(lines):
    if line.strip().startswith('async def '):
        func_name = line.split('async def ')[1].split('(')[0]
        async_funcs.append((func_name, i+1))

print(f"\n✓ ASYNC ENTRY POINTS (main signal loop): {len(async_funcs)}")
for func_name, lineno in sorted(async_funcs):
    print(f"  • {func_name:50s} (line {lineno})")

# Look for signal evaluation functions
eval_lines = []
for i, line in enumerate(lines):
    if any(kw in line.lower() for kw in ['if not signal', 'if signal', 'quality_gate', 'evaluate_signal', 'accept_trade', 'reject_trade', 'should_trade']):
        eval_lines.append((i+1, line.strip()[:80]))

print(f"\n✓ KEY DECISION POINTS (signal acceptance/rejection): {len(eval_lines)}")
for lineno, line in eval_lines[:20]:
    print(f"  Line {lineno:5d}: {line}")

# CRITICAL ANALYSIS: Gate logic
print("\n" + "=" * 100)
print("GATE LOGIC ANALYSIS - Why are 64% of trades losing?")
print("=" * 100)

# Find where quality_gate is applied
gate_analysis = []
for i, line in enumerate(lines):
    if 'quality_gate' in line.lower() and ('if' in line or 'not' in line or 'filter' in line):
        gate_analysis.append({
            'lineno': i+1,
            'code': line.strip()[:100],
            'context_before': lines[max(0, i-2)].strip()[:80] if i > 0 else '',
            'context_after': lines[min(len(lines)-1, i+2)].strip()[:80] if i < len(lines)-1 else ''
        })

print(f"\n✓ QUALITY GATE USAGE LOCATIONS: {len(gate_analysis)}")
for item in gate_analysis[:15]:
    print(f"\n  Line {item['lineno']}:")
    print(f"    Context: {item['context_before']}")
    print(f"    ► {item['code']}")
    print(f"    Follows: {item['context_after']}")

# CRITICAL: Find where entry price, stop loss, take profit are calculated
print("\n" + "=" * 100)
print("ENTRY/EXIT LOGIC ANALYSIS - How are stops and targets set?")
print("=" * 100)

entry_logic = []
for i, line in enumerate(lines):
    if any(kw in line.lower() for kw in ['stop_loss', 'take_profit', 'entry_price', 'tp1', 'tp2', 'sl']):
        if '=' in line and not line.strip().startswith('#'):
            entry_logic.append({
                'lineno': i+1,
                'code': line.strip()[:120]
            })

print(f"\n✓ ENTRY/EXIT CALCULATION POINTS: {len(entry_logic)}")
for item in entry_logic[:30]:
    print(f"  Line {item['lineno']:5d}: {item['code']}")

# CRITICAL: Analyze swing/structure detection
print("\n" + "=" * 100)
print("SETUP DETECTION LOGIC - How are sweeps and breakouts identified?")
print("=" * 100)

setup_logic = []
for i, line in enumerate(lines):
    if any(kw in line.lower() for kw in ['sweep', 'breakout', 'swing_high', 'swing_low', 'structure']):
        if 'def ' in line or '=' in line:
            setup_logic.append({
                'lineno': i+1,
                'code': line.strip()[:120]
            })

print(f"\n✓ SETUP DETECTION LOCATIONS: {len(setup_logic)}")
for item in setup_logic[:30]:
    print(f"  Line {item['lineno']:5d}: {item['code']}")

# CRITICAL: Order flow scoring
print("\n" + "=" * 100)
print("ORDER FLOW ANALYSIS - How is OF signal evaluated?")
print("=" * 100)

of_logic = []
for i, line in enumerate(lines):
    if any(kw in line.lower() for kw in ['order_flow', 'of_', 'pressure', 'imbalance', 'cvd']):
        if 'def ' in line or ('=' in line and not line.strip().startswith('#')):
            of_logic.append({
                'lineno': i+1,
                'code': line.strip()[:120]
            })

print(f"\n✓ ORDER FLOW CALCULATION POINTS: {len(of_logic)}")
for item in of_logic[:30]:
    print(f"  Line {item['lineno']:5d}: {item['code']}")

# CRITICAL: Execution flow
print("\n" + "=" * 100)
print("EXECUTION FLOW ANALYSIS - What happens when a signal triggers?")
print("=" * 100)

execution_logic = []
for i, line in enumerate(lines):
    if any(kw in line.lower() for kw in ['place_order', 'execute', 'submit', 'send_signal', 'trade_logic', 'main_loop']):
        if 'def ' in line:
            execution_logic.append({
                'lineno': i+1,
                'code': line.strip()[:120],
                'snippet': '\n'.join([lines[j].strip()[:100] for j in range(i, min(i+5, len(lines)))])
            })

print(f"\n✓ EXECUTION/ORDER PLACEMENT POINTS: {len(execution_logic)}")
for item in execution_logic[:10]:
    print(f"\n  Line {item['lineno']:5d}: {item['code']}")

print("\n" + "=" * 100)
print("AUDIT SUMMARY - KEY FINDINGS")
print("=" * 100)

print(f"""
SIGNAL LOGIC STRUCTURE:
  • Total Functions: {len(functions)}
  • Async Entry Points: {len(async_funcs)}
  • Gate Decision Points: {len(gate_analysis)}
  • Setup Detection Locations: {len(setup_logic)}
  • Order Flow Analysis Locations: {len(of_logic)}
  • Execution Flow Points: {len(execution_logic)}

CRITICAL ISSUES TO INVESTIGATE:
  1. Where is quality_gate ACTUALLY applied? (gate_analysis above)
  2. Are signals being accepted BEFORE gate validation? (look at order of operations)
  3. How is entry_price calculated? Is it using current price? (look at entry_logic)
  4. Stop loss placement: Is it using ATR correctly? (check SL calculation)
  5. Order flow: Is OF score actually filtered or just logged? (check of_logic usage)
  6. Setup detection: Is sweep/breakout identification correct? (check structure detection)
  7. Are rejected trades being tracked separately? (check audit logic)

NEXT STEPS:
  → Read the main async loop to understand signal flow
  → Check where trades are ACTUALLY placed vs where they're evaluated
  → Compare audit logs for accepted vs rejected vs executed trades
  → Look for logical OR when should be AND (over-permissive logic)
  → Check for off-by-one errors in timeframe calculations
""")

print("\n✓ Use grep to search specific functions in signal_analyzer.py")
print("  Example: Search for 'async def' to find main loop")
print("  Example: Search for 'quality_gate.evaluate' to find gate application")
print("  Example: Search for 'place_order' to find execution")
