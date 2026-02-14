"""
Generate accuracy report comparing detected vs planted anomalies
"""

import json
import pandas as pd
from pathlib import Path


def generate_accuracy_report():
    """Compare detected anomalies against planted anomalies"""
    
    # Load planted anomalies
    with open('../data/planted_anomalies.json', 'r') as f:
        planted = json.load(f)
    
    # Load detected anomalies from main report
    with open('../output/anomaly_report.json', 'r') as f:
        report = json.load(f)
        detected = report['anomalies']
    
    # Create mapping of planted: (shipment_id, anomaly_type) -> full record
    planted_map = {}
    for anom in planted:
        sid = anom.get('shipment_id')
        # Skip if no shipment_id or if it's a multi-shipment pattern
        if sid is None or isinstance(sid, str) and '-' in str(sid):
            continue
        atype = anom.get('anomaly_type', '')
        key = (sid, atype)
        planted_map[key] = anom
    
    # Create mapping of detected: (shipment_id, anomaly_type) -> full record
    detected_map = {}
    for anom in detected:
        sid = anom.get('shipment_id')
        if sid is None:
            continue
        atype = anom.get('anomaly_type', '')
        key = (sid, atype)
        detected_map[key] = anom
    
    # Find matches
    detected_correctly = 0
    correctly_detected_list = []
    
    for key in planted_map:
        if key in detected_map:
            detected_correctly += 1
            correctly_detected_list.append({
                'shipment_id': key[0],
                'anomaly_type': key[1],
                'severity': planted_map[key].get('severity', 'N/A')
            })
    
    # Find missed anomalies
    missed = []
    missed_details = []
    for key, anom in planted_map.items():
        if key not in detected_map:
            missed.append(f"Shipment {key[0]}: {key[1]}")
            missed_details.append({
                'shipment_id': key[0],
                'anomaly_type': key[1],
                'severity': anom.get('severity', 'N/A'),
                'description': anom.get('description', '')
            })
    
    # Find false positives (detected but not planted)
    false_positives = 0
    false_positive_details = []
    
    for key, anom in detected_map.items():
        if key not in planted_map:
            false_positives += 1
            false_positive_details.append({
                'shipment_id': key[0],
                'anomaly_type': key[1],
                'layer': anom.get('layer', 'N/A'),
                'severity': anom.get('severity', 'N/A'),
                'evidence': str(anom.get('evidence', {}))[:200],  # Truncate for readability
                'why_flagged': anom.get('impact', 'N/A')
            })
    
    # Calculate metrics
    total_planted = len(planted)
    total_detected = len(detected)
    
    # Precision = TP / (TP + FP)
    precision = detected_correctly / (detected_correctly + false_positives) if (detected_correctly + false_positives) > 0 else 0.0
    
    # Recall = TP / (TP + FN)
    recall = detected_correctly / total_planted if total_planted > 0 else 0.0
    
    # F1 = 2 * (precision * recall) / (precision + recall)
    f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
    
    # Build report
    accuracy_report = {
        'metadata': {
            'total_planted_anomalies': total_planted,
            'total_detected_anomalies': total_detected,
            'correctly_detected': detected_correctly,
            'missed_anomalies_count': len(missed),
            'false_positives_count': false_positives
        },
        'metrics': {
            'precision': round(precision, 4),
            'recall': round(recall, 4),
            'f1_score': round(f1_score, 4),
            'accuracy': round(detected_correctly / total_detected, 4) if total_detected > 0 else 0.0
        },
        'performance': {
            'detected_correctly': detected_correctly,
            'missed': len(missed),
            'false_positives': false_positives
        },
        'correctly_detected_anomalies': correctly_detected_list,
        'missed_anomalies': missed_details,
        'false_positive_details': false_positive_details,
        'summary': {
            'correct_detections': f"{detected_correctly}/{total_planted}",
            'detection_rate': f"{round(100*recall, 1)}%",
            'false_positive_rate': f"{round(100*false_positives/total_detected, 1)}%" if total_detected > 0 else "N/A"
        }
    }
    
    # Save report
    output_path = Path('../output/accuracy_report.json')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(accuracy_report, f, indent=2)
    
    print("="*70)
    print("ACCURACY REPORT GENERATED")
    print("="*70)
    print(f"\nPlanted Anomalies: {total_planted}")
    print(f"Detected Anomalies: {total_detected}")
    print(f"\nResults:")
    print(f"  Correctly Detected: {detected_correctly}/{total_planted}")
    print(f"  Missed: {len(missed)}")
    print(f"  False Positives: {false_positives}")
    print(f"\nMetrics:")
    print(f"  Precision: {round(precision, 4)}")
    print(f"  Recall: {round(recall, 4)}")
    print(f"  F1 Score: {round(f1_score, 4)}")
    print(f"\nReport saved to: output/accuracy_report.json")
    
    if missed:
        print(f"\nMissed Anomalies ({len(missed)}):")
        for miss in missed_details:
            print(f"  - Shipment {miss['shipment_id']}: {miss['anomaly_type']} ({miss['severity']})")
    
    if false_positive_details:
        print(f"\nFalse Positives ({len(false_positive_details)}):")
        for fp in false_positive_details[:5]:  # Show first 5
            print(f"  - Shipment {fp['shipment_id']}: {fp['anomaly_type']} (Layer {fp['layer']})")
        if len(false_positive_details) > 5:
            print(f"  ... and {len(false_positive_details) - 5} more")
    
    print("\n" + "="*70)


if __name__ == "__main__":
    try:
        generate_accuracy_report()
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please run the detection pipeline first to generate anomaly_report.json")
