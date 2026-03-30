#!/usr/bin/env python3
"""Update flags: 0 -> correct computed flag values in test assertions."""

with open('crates/skeindb/src/server.rs', 'r') as f:
    lines = f.readlines()

updates = {
    32800: [20207, 20238, 20296, 20324, 20352, 20357, 20362, 20390, 20395, 20444, 20449],
    32768: [20477, 20487, 20538, 20543, 20548, 20592, 20597, 20638, 20643, 20669, 20674, 20700, 20705, 20710, 20746, 20756, 20761],
    32803: [20872, 20894, 20946, 20961, 20976, 20991],
    32769: [20841, 20856, 20877, 20899],
}

changed = 0
for new_val, line_numbers in updates.items():
    for lno in line_numbers:
        idx = lno - 1
        old = lines[idx]
        if 'flags: 0,' in old:
            lines[idx] = old.replace('flags: 0,', f'flags: {new_val},', 1)
            changed += 1
        elif 'flags: 0\n' in old:
            lines[idx] = old.replace('flags: 0\n', f'flags: {new_val}\n', 1)
            changed += 1

print(f'Changed {changed} lines')

with open('crates/skeindb/src/server.rs', 'w') as f:
    f.writelines(lines)
