"""
OPTIMIZED LAYER 3: LLM-Based Anomaly Detection
- Reduces buyer anomaly false positives
- Only detects HS code mismatches and critical patterns
- Strict validation to avoid over-flagging
"""

import pandas as pd
import json
import os
import sys
import numpy as np
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timedelta
import hashlib
import time

if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


class OptimizedLLMDetector:
    """Enhanced LLM detector with strict anomaly validation"""
    
    def __init__(
        self,
        shipments_df: pd.DataFrame,
        product_catalog: pd.DataFrame,
        buyers_df: pd.DataFrame,
        llm_client=None,
        cache_responses: bool = True
    ):
        self.df = shipments_df.copy()
        self.product_catalog = product_catalog
        self.buyers_df = buyers_df
        self.llm_client = llm_client
        self.cache_responses = cache_responses
        
        self.anomalies = []
        self.llm_calls = 0
        self.response_cache = {}
        self.total_tokens = {'input': 0, 'output': 0}
        self.task_metrics = {
            'hs_code_validation': {'calls': 0, 'tokens': 0, 'skipped': 0, 'findings': 0},
            'executive_summary': {'calls': 0, 'tokens': 0}
        }
        
        self._build_lookup_tables()

    def _build_lookup_tables(self):
        """Pre-compute lookup tables"""
        self.product_lookup = self.product_catalog.set_index('id').to_dict('index')
        self.buyer_lookup = self.buyers_df.set_index('id').to_dict('index')
        print("   ✓ Lookup tables built")

    def _get_cache_key(self, data: Dict) -> str:
        """Generate hash-based cache key"""
        json_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.md5(json_str.encode()).hexdigest()

    def _estimate_tokens(self, text: str) -> int:
        """Estimate tokens: 1 token ≈ 1.3 words"""
        return max(1, int(len(text.split()) * 1.3))

    # =========================================================================
    # DETECTION 1: HS CODE VALIDATION (ONLY - High Precision)
    # =========================================================================

    def validate_hs_codes_strict(self, batch_size: int = 25):
        """
        Validate HS codes with HIGH PRECISION only.
        Only flag clear mismatches, NOT marginal cases.
        """
        print("  1. Validating HS codes (HIGH PRECISION MODE)...")
        
        if not self.llm_client:
            print("   ⚠️  No LLM client. Skipping.")
            return

        # Only check high-value shipments (top 10% by FOB)
        threshold = self.df['total_fob'].quantile(0.90)
        sample = self.df[self.df['total_fob'] >= threshold].copy()
        
        print(f"   • Screening {len(sample)} high-value shipments")
        
        sample_with_products = self._enrich_with_products(sample)
        
        # Pre-filter: skip obvious matches
        uncertain = [
            s for s in sample_with_products 
            if not self._is_obvious_match(s)
        ]
        
        if uncertain:
            print(f"   • Found {len(uncertain)} uncertain cases")
        else:
            print("   ✓ All HS codes appear valid")
            return

        # Batch validation
        for i in range(0, len(uncertain), batch_size):
            batch = uncertain[i:i + batch_size]
            self._validate_hs_batch_strict(batch)

    def _enrich_with_products(self, shipment_batch: pd.DataFrame) -> List[Dict]:
        """Enrich shipments with product info"""
        enriched = []
        for _, row in shipment_batch.iterrows():
            product_id = int(row['product_id'])
            if product_id in self.product_lookup:
                p = self.product_lookup[product_id]
                enriched.append({
                    "shipment_id": int(row['id']),
                    "hs_code": str(row['hs_code']),
                    "product_name": str(p.get('name', 'Unknown')),
                    "description": str(p.get('description', 'N/A')),
                    "material": str(p.get('material', 'Unknown')),
                    "category": str(p.get('category', 'Unknown')),
                    "fob_value": float(row['total_fob'])
                })
        return enriched

    def _is_obvious_match(self, shipment: Dict) -> bool:
        """Quick check: if HS code clearly matches, skip LLM"""
        hs_code = str(shipment.get('hs_code', '')).strip()
        product = str(shipment.get('product_name', '')).lower()
        material = str(shipment.get('material', '')).lower()
        category = str(shipment.get('category', '')).lower()
        
        text = f"{product} {material} {category}".lower()
        
        # Clear matches by prefix
        rules = {
            '61': ['shirt', 'knit', 'sweater', 'jersey', 'apparel', 'garment'],
            '62': ['apparel', 'clothing', 'fabric', 'textile', 'cotton', 'dress'],
            '69': ['ceramic', 'tile', 'pottery', 'clay'],
            '73': ['fastener', 'bolt', 'screw', 'nut', 'stainless', 'metal'],
            '72': ['iron', 'steel', 'metal bar', 'plate', 'coil', 'rod'],
            '84': ['machine', 'engine', 'motor', 'pump', 'compressor', 'equipment'],
            '85': ['electric', 'electronic', 'led', 'light', 'circuit', 'transformer'],
            '94': ['chair', 'furniture', 'wood', 'teak', 'sofa', 'table', 'desk']
        }
        
        prefix = hs_code[:2] if len(hs_code) >= 2 else ''
        if prefix in rules:
            return any(kw in text for kw in rules[prefix])
        
        return False

    def _validate_hs_batch_strict(self, batch: List[Dict]):
        """Strict HS code validation with high precision"""
        batch_data = batch[:15]  # Limit to 15 per call for quality
        
        cache_key = self._get_cache_key({'type': 'hs_strict', 'ids': [s['shipment_id'] for s in batch_data]})
        if self.cache_responses and cache_key in self.response_cache:
            self._process_hs_results_strict(self.response_cache[cache_key])
            self.task_metrics['hs_code_validation']['skipped'] += len(batch_data)
            return

        prompt = f"""TASK: Identify ONLY clear HS code mismatches (high confidence only).

STRICT CRITERIA:
- Flag only if HS code category CONTRADICTS product material
- Do NOT flag if code reasonably fits (e.g., teak wood with furniture code is OK)
- Do NOT flag if material vaguely relates to code
- Require: DEFINITE conflict between claimed code and actual material

SHIPMENTS TO VALIDATE:
{json.dumps(batch_data, ensure_ascii=False)}

HS CODE CATEGORIES:
- 61: Knitted apparel ONLY (shirts, sweaters, jersey)
- 62: Woven apparel ONLY (dresses, fabric, textiles)
- 69: Ceramics ONLY (tiles, pottery)
- 73: Iron/steel fasteners ONLY (bolts, screws, nuts)
- 84: Machinery ONLY (motors, pumps, equipment)
- 85: Electronics ONLY (LED lights, circuits, electrical)
- 94: Wooden furniture ONLY (chairs, tables, desks)

RESPOND ONLY WITH JSON (no other text):
[
  {{
    "shipment_id": 1,
    "is_mismatch": true,
    "confidence": 0.95,
    "reason": "HS code 84 (machinery) but material is fabric"
  }},
  {{
    "shipment_id": 2,
    "is_mismatch": false,
    "confidence": 1.0,
    "reason": "null"
  }}
]

RETURN ONLY MISMATCHES WITH confidence >= 0.8"""

        try:
            call_start = time.time()
            response = self.llm_client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[
                    {"role": "system", "content": "You are a customs expert. Only flag DEFINITE HS code mismatches. High precision required."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,  # Low temp for consistency
                max_tokens=500
            )
            
            content = response.choices[0].message.content.strip()
            
            # Parse JSON
            try:
                start_idx = content.find('[')
                end_idx = content.rfind(']') + 1
                results = json.loads(content[start_idx:end_idx]) if start_idx >= 0 else []
            except:
                results = []

            # Only keep high confidence mismatches
            mismatches = [r for r in results if r.get('is_mismatch') and r.get('confidence', 0) >= 0.8]
            
            if mismatches:
                print(f"   ✓ Found {len(mismatches)} definite HS code mismatches")
                self._process_hs_results_strict(mismatches)
            
            # Token accounting
            self.llm_calls += 1
            input_tokens = self._estimate_tokens(prompt)
            output_tokens = self._estimate_tokens(content)
            self.total_tokens['input'] += input_tokens
            self.total_tokens['output'] += output_tokens
            self.task_metrics['hs_code_validation']['calls'] += 1
            self.task_metrics['hs_code_validation']['tokens'] += input_tokens + output_tokens
            self.task_metrics['hs_code_validation']['findings'] += len(mismatches)
            
            self.response_cache[cache_key] = mismatches

        except Exception as e:
            print(f"   ⚠️  HS validation error: {str(e)[:50]}")

    def _process_hs_results_strict(self, mismatches: List[Dict]):
        """Process ONLY definite mismatches"""
        for match in mismatches:
            self.anomalies.append({
                'shipment_id': match.get('shipment_id'),
                'anomaly_type': 'HS_CODE_PRODUCT_MISMATCH',
                'layer': 3,
                'severity': 'HIGH',
                'confidence': match.get('confidence', 0.8),
                'description': f"HS code mismatch: {match.get('reason', 'Product-code conflict')}",
                'evidence': match,
                'recommendation': 'Verify HS code with customs broker before shipment'
            })

    # =========================================================================
    # NO BUYER PATTERN DETECTION - TOO MANY FALSE POSITIVES
    # =========================================================================
    
    def analyze_buyer_patterns_disabled(self):
        """
        DISABLED: Buyer pattern detection causes too many false positives.
        Only use if you have labeled training data for your buyers.
        """
        print("  2. Buyer pattern analysis: DISABLED (high false positive rate)")
        print("     → Re-enable only with labeled buyer fraud data")

    # =========================================================================
    # DETECTION 2: TRADE COMPLIANCE (MINIMAL - Route delays only)
    # =========================================================================

    def check_trade_compliance_minimal(self):
        """
        MINIMAL compliance check: extreme transit delays ONLY.
        Other checks can be added when validated against labeled data.
        """
        print("  2. Checking trade compliance (extreme delays only)...")
        
        if not self.llm_client:
            return

        # Only extreme outliers (3+ standard deviations)
        mean_transit = self.df['days_in_transit'].mean()
        std_transit = self.df['days_in_transit'].std()
        threshold = mean_transit + (3 * std_transit)
        
        extreme = self.df[self.df['days_in_transit'] > threshold]
        
        if len(extreme) == 0:
            print("   ✓ No extreme transit delays detected")
            return

        print(f"   • Found {len(extreme)} extreme transit delays")
        
        # Flag them directly without LLM (clear pattern)
        for _, row in extreme.iterrows():
            self.anomalies.append({
                'shipment_id': int(row['id']),
                'anomaly_type': 'EXTREME_TRANSIT_DELAY',
                'layer': 3,
                'severity': 'MEDIUM',
                'description': f"Extreme transit delay: {int(row['days_in_transit'])} days (normal: {int(mean_transit)} ± {int(std_transit)})",
                'evidence': {
                    'actual_days': int(row['days_in_transit']),
                    'mean_transit': round(mean_transit, 2),
                    'std_transit': round(std_transit, 2),
                    'threshold': round(threshold, 2)
                },
                'recommendation': 'Investigate shipment delay with logistics provider'
            })

    # =========================================================================
    # EXECUTIVE SUMMARY (OPTIONAL)
    # =========================================================================

    def generate_executive_summary(self, anomalies: List[Dict], shipments_df: pd.DataFrame) -> Optional[str]:
        """Generate 1-page executive summary"""
        if not self.llm_client or not anomalies:
            return None

        print("  3. Generating executive summary...")

        critical = [a for a in anomalies if a.get('severity') == 'CRITICAL']
        high = [a for a in anomalies if a.get('severity') == 'HIGH']
        
        summary_text = f"""
CRITICAL ISSUES: {len(critical)}
HIGH PRIORITY: {len(high)}
TOTAL ANOMALIES: {len(anomalies)}

ANOMALY BREAKDOWN:
"""
        by_type = {}
        for a in anomalies:
            atype = a.get('anomaly_type', 'Unknown')
            by_type[atype] = by_type.get(atype, 0) + 1
        
        for atype, count in sorted(by_type.items(), key=lambda x: x[1], reverse=True):
            summary_text += f"\n- {atype}: {count}"

        prompt = f"""Write a brief executive summary (100-150 words) about shipment anomalies.
Focus on: 1) Top risks, 2) Financial impact, 3) Recommended actions.

DATA:
{summary_text}

Use plain business language. Start with: # Executive Summary"""

        try:
            response = self.llm_client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                max_tokens=300
            )

            content = response.choices[0].message.content.strip()
            
            self.llm_calls += 1
            input_tokens = self._estimate_tokens(prompt)
            output_tokens = self._estimate_tokens(content)
            self.total_tokens['input'] += input_tokens
            self.total_tokens['output'] += output_tokens
            self.task_metrics['executive_summary']['calls'] += 1
            self.task_metrics['executive_summary']['tokens'] += input_tokens + output_tokens
            
            return content

        except Exception as e:
            print(f"   ⚠️  Summary error: {str(e)[:50]}")
            return None

    # =========================================================================
    # MAIN ENTRY POINT
    # =========================================================================

    def run_all_detections(self) -> Tuple[List[Dict], int, Dict]:
        """Run all detections"""
        print("\n🟢 Running Layer 3: Optimized LLM Detection (High Precision)")
        print("   - HS code validation ONLY")
        print("   - Buyer detection DISABLED (reduces false positives)")
        print("   - Compliance checks MINIMAL (only extreme cases)")
        
        if not self.llm_client:
            print("   ⚠️  No LLM client. Skipping Layer 3.")
            return [], 0, self.total_tokens

        self.validate_hs_codes_strict(batch_size=15)
        self.analyze_buyer_patterns_disabled()
        self.check_trade_compliance_minimal()

        print(f"\n   ✓ Layer 3 complete: {len(self.anomalies)} anomalies found")
        print(f"   • LLM calls: {self.llm_calls}")
        print(f"   • Tokens: {self.total_tokens['input'] + self.total_tokens['output']}")

        return self.anomalies, self.llm_calls, self.total_tokens

    def print_metrics(self):
        """Print metrics"""
        print("\n📊 Layer 3 Metrics:")
        for task, metrics in self.task_metrics.items():
            calls = metrics['calls']
            if calls > 0:
                print(f"   {task}:")
                print(f"      Calls: {calls} | Tokens: {metrics['tokens']} | Findings: {metrics.get('findings', 'N/A')}")


# ============================================================================
# PIPELINE INTEGRATION
# ============================================================================

class OptimizedAnomalyPipeline:
    """Full pipeline with L1, L2, L3"""
    
    def __init__(self, shipments_df, product_catalog, routes_df, buyers_df, llm_client=None):
        self.df = shipments_df
        self.product_catalog = product_catalog
        self.routes_df = routes_df
        self.buyers_df = buyers_df
        self.llm_client = llm_client
        self.all_anomalies = []
        self.llm_calls = 0
        self.execution_time = None
        self.total_tokens = {'input': 0, 'output': 0}

    def run_full_analysis(self):
        """Run all layers"""
        from datetime import datetime
        start = datetime.now()

        print("="*70)
        print("SHIPMENT ANOMALY DETECTION - 3 LAYER PIPELINE")
        print("="*70)

        # Layer 1
        print("\n🔴 Layer 1: Rule-Based Detection")
        try:
            from rule_engine import RuleEngine
            rule_engine = RuleEngine(self.df, self.product_catalog, self.routes_df, self.buyers_df)
            l1_anomalies = rule_engine.run_all_rules()
        except ImportError:
            print("   ⚠️  Rule engine not available")
            l1_anomalies = []
        
        self.all_anomalies.extend(l1_anomalies)

        # Layer 2
        print("\n🟡 Layer 2: Statistical Detection")
        try:
            from statistical_detector import StatisticalDetector
            stat_detector = StatisticalDetector(self.df, self.product_catalog, self.routes_df, self.buyers_df)
            l2_anomalies = stat_detector.run_all_detections()
        except ImportError:
            print("   ⚠️  Statistical detector not available")
            l2_anomalies = []
        
        self.all_anomalies.extend(l2_anomalies)

        # Layer 3 (Optimized)
        print("\n🟢 Layer 3: LLM Detection (Optimized)")
        llm_detector = OptimizedLLMDetector(
            self.df, self.product_catalog, self.buyers_df,
            self.llm_client, cache_responses=True
        )
        l3_anomalies, l3_calls, l3_tokens = llm_detector.run_all_detections()
        self.all_anomalies.extend(l3_anomalies)
        self.llm_calls = l3_calls
        self.total_tokens = l3_tokens

        llm_detector.print_metrics()

        self.execution_time = (datetime.now() - start).total_seconds()

        # Summary
        print("\n" + "="*70)
        print("📊 ANALYSIS SUMMARY")
        print("="*70)
        print(f"Layer 1 (Rules):        {len(l1_anomalies)} anomalies")
        print(f"Layer 2 (Statistics):   {len(l2_anomalies)} anomalies")
        print(f"Layer 3 (LLM):          {len(l3_anomalies)} anomalies")
        print(f"━" * 70)
        print(f"TOTAL:                  {len(self.all_anomalies)} anomalies")
        print(f"\nExecution time: {self.execution_time:.2f}s")
        print(f"LLM calls: {self.llm_calls}")
        print(f"Tokens: {self.total_tokens['input'] + self.total_tokens['output']}")

        # KEY FIX: Return tuple (anomalies, llm_calls) - NOT just anomalies
        return self.all_anomalies, self.llm_calls

    def generate_reports(self, output_dir='../output'):
        """Save reports"""
        os.makedirs(output_dir, exist_ok=True)

        # Anomaly report
        report = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'execution_time': self.execution_time,
                'total_anomalies': len(self.all_anomalies),
                'llm_calls': self.llm_calls
            },
            'anomalies': self.all_anomalies
        }

        with open(f'{output_dir}/anomaly_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, default=str, ensure_ascii=False)
        
        print(f"✓ Report saved: {output_dir}/anomaly_report.json")

        # Summary
        if self.all_anomalies:
            df_anom = pd.DataFrame(self.all_anomalies)
            summary = {
                'by_type': df_anom['anomaly_type'].value_counts().to_dict(),
                'by_severity': df_anom['severity'].value_counts().to_dict() if 'severity' in df_anom else {}
            }
            with open(f'{output_dir}/anomaly_summary.json', 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    print("Optimized LLM Detector loaded.")
    print("\nKEY CHANGES:")
    print("✓ HS code validation: HIGH PRECISION (confidence >= 0.8)")
    print("✓ Buyer detection: DISABLED (causes false positives)")
    print("✓ Compliance: MINIMAL (extreme delays only)")
    print("✓ Result: Fewer false positives, higher precision")

