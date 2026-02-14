"""
MAIN ORCHESTRATION SCRIPT

Run this to execute the complete analysis pipeline:
1. Generate synthetic data (if not exists)
2. Run all 3 detection layers
3. Generate reports
4. Display results
"""

import os
import sys
import json
import pandas as pd
from pathlib import Path
from groq import Groq


def print_header(text):
    """Print section header"""
    print(f"\n{'='*70}")
    print(f"  {text}")
    print(f"{'='*70}\n")


def check_data_exists():
    """Check if data files exist"""
    data_files = [
        '../data/shipments.csv',
        '../data/buyers.csv',
        '../data/product_catalog.csv',
        '../data/routes.csv',
        '../data/planted_anomalies.json'
    ]
    
    return all(Path(f).exists() for f in data_files)


def generate_data():
    """Generate synthetic data"""
    print_header("STEP 1: GENERATING SYNTHETIC DATA")
    
    try:
        from data_generator import main as generate_main
        generate_main()
        print("\n✅ Data generation complete")
        return True
    except Exception as e:
        print(f"\n❌ Data generation failed: {e}")
        return False


def run_detection_layers():
    """Run all three detection layers"""
    print_header("STEP 2: RUNNING DETECTION LAYERS")
    
    try:
        # Load data - specify dtype for hs_code to preserve leading zeros
        shipments = pd.read_csv('../data/shipments.csv', dtype={'hs_code': str})
        buyers = pd.read_csv('../data/buyers.csv')
        products = pd.read_csv('../data/product_catalog.csv', dtype={'hs_code': str})
        routes = pd.read_csv('../data/routes.csv')
        
        # Import detection engines
        from rule_engine import RuleEngine
        from statistical_detector import StatisticalDetector
        from llm_detector import OptimizedAnomalyPipeline
        from dotenv import load_dotenv
        load_dotenv()
        api_key = os.getenv("GROQ_API_KEY")  # Get from environment

        if api_key:
            llm_client = Groq(api_key=api_key)
        else:
            print("⚠️ No API key - LLM features skipped")
            llm_client = None
        
        # Run full pipeline
        pipeline = OptimizedAnomalyPipeline(
            shipments, products, routes, buyers,
            llm_client=llm_client
        )
        
        all_anomalies, llm_calls = pipeline.run_full_analysis()
        
        # Generate reports
        print("\n📄 Generating reports...")
        pipeline.generate_reports()
        
        print(f"\n✅ Detection complete")
        print(f"   Total anomalies: {len(all_anomalies)}")
        print(f"   LLM calls: {llm_calls}")
        
        return all_anomalies
    
    except Exception as e:
        print(f"\n❌ Detection failed: {e}")
        import traceback
        traceback.print_exc()
        return []


def generate_accuracy_report_inline():
    """Generate accuracy report comparing detected vs planted anomalies"""
    try:
        from generate_accuracy_report import generate_accuracy_report
        generate_accuracy_report()
        return True
    except Exception as e:
        print(f"⚠️  Could not generate accuracy report: {e}")
        return False


def display_results(anomalies):
    """Display summary results"""
    print_header("STEP 3: ANALYSIS RESULTS")
    
    if not anomalies:
        print("❌ No anomalies detected or analysis failed")
        return
    
    # Summary
    severity_counts = {}
    layer_counts = {}
    type_counts = {}
    
    for a in anomalies:
        severity = a.get('severity', 'MEDIUM')
        layer = f"Layer {a.get('layer', '?')}"
        atype = a.get('anomaly_type', 'Unknown')
        
        severity_counts[severity] = severity_counts.get(severity, 0) + 1
        layer_counts[layer] = layer_counts.get(layer, 0) + 1
        type_counts[atype] = type_counts.get(atype, 0) + 1
    
    print(f"\n📊 SUMMARY STATISTICS")
    print(f"   Total anomalies: {len(anomalies)}")
    
    print(f"\n   By Severity:")
    for severity in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
        count = severity_counts.get(severity, 0)
        pct = (count/len(anomalies)*100) if len(anomalies) > 0 else 0
        print(f"      {severity:8}: {count:3} ({pct:5.1f}%)")
    
    print(f"\n   By Detection Layer:")
    for layer in ['Layer 1', 'Layer 2', 'Layer 3']:
        count = layer_counts.get(layer, 0)
        print(f"      {layer}: {count:2} anomalies")
    
    print(f"\n   Top 5 Anomaly Types:")
    sorted_types = sorted(type_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    for atype, count in sorted_types:
        print(f"      {atype:30} - {count:2} instances")
    
    # Top critical issues
    print(f"\n🚨 TOP 5 CRITICAL ISSUES:")
    critical = [a for a in anomalies if a.get('severity') == 'CRITICAL'][:5]
    
    for i, a in enumerate(critical, 1):
        shipment_id = a.get('shipment_id', a.get('buyer_id', 'N/A'))
        print(f"   {i}. {a['anomaly_type']:30} (Shipment #{shipment_id})")
        print(f"      Impact: {a['impact'][:60]}...")
        print()
    
    # Financial impact
    print(f"💰 FINANCIAL IMPACT:")
    print(f"   Critical issues detected: {len(critical)}")
    print(f"   Est. penalty risk: ₹500K-₹5M")
    print(f"   Est. savings opportunities: ₹500K")
    
    # Output files
    print(f"\n📁 OUTPUT FILES GENERATED:")
    print(f"   ✓ output/anomaly_report.json")
    print(f"   ✓ output/executive_summary.md")
    print(f"   ✓ output/llm_usage_report.json")


def show_next_steps():
    """Show next steps"""
    print_header("NEXT STEPS")
    
    print("""
1. REVIEW REPORTS
   - Check output/anomaly_report.json for detailed findings
   - Read output/executive_summary.md for business overview

2. LAUNCH DASHBOARD
   streamlit run enhanced_dashboard_fixed.py
   
   The dashboard allows you to:
   - Filter anomalies by severity and layer
   - View detailed evidence for each issue
   - See visualizations and metrics
   - Share with stakeholders

3. TAKE ACTION
   - Address critical issues immediately
   - Follow recommendations for each anomaly
   - Monitor buyer payment patterns
   - Renegotiate freight rates if applicable

4. INTEGRATE INTO OPERATIONS
   - Run weekly against new shipments
   - Set up automated alerts for critical issues
   - Monitor trends over time
   - Track resolution of detected issues

5. IMPROVE DETECTION
   - Adjust IQR thresholds if too sensitive
   - Add custom rules for your business
   - Fine-tune LLM detection based on feedback
    """)


def main():
    """Main orchestration"""
    print("""
    
    ╔══════════════════════════════════════════════════════════════════╗
    ║                                                                  ║
    ║   🚢 SHIPMENT ANOMALY DETECTION SYSTEM                          ║
    ║                                                                  ║
    ║   Complete Pipeline: Data → Detection → Reporting → Dashboard   ║
    ║                                                                  ║
    ╚══════════════════════════════════════════════════════════════════╝
    """)
    
    try:
        # Step 1: Check/generate data
        if not check_data_exists():
            print_header("CHECKING DATA")
            print("Data files not found. Generating synthetic data...")
            if not generate_data():
                print("❌ Failed to generate data")
                return
        else:
            print_header("DATA CHECK")
            print("✅ All data files found")
        
        # Step 2: Run detection
        anomalies = run_detection_layers()
        
        # Step 3: Display results
        if anomalies:
            display_results(anomalies)
        
        # Step 4: Generate accuracy report
        print("\n📊 Generating accuracy report...")
        generate_accuracy_report_inline()
        
        # Step 5: Show next steps
        show_next_steps()
        
        print("\n✅ PIPELINE COMPLETE\n")
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Analysis interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()