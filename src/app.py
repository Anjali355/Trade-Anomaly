import streamlit as st
import pandas as pd
import json
import os
import subprocess
import time
from datetime import datetime
from pathlib import Path
import plotly.graph_objects as go
import plotly.express as px
import threading
from typing import Dict, List, Any

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="Anomaly Detection Dashboard",
    page_icon="🚢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    :root {
        --primary: #0f4c75;
        --accent: #d32f2f;
        --success: #2e7d32;
        --warning: #f57c00;
        --info: #0288d1;
        --dark: #1a1a1a;
    }
    
    * {
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    
    .main { background: #f8f9fa; }
    
    .metric-card {
        background: linear-gradient(135deg, #ffffff 0%, #f5f7fa 100%);
        border-left: 4px solid var(--primary);
        padding: 24px;
        border-radius: 8px;
        margin: 12px 0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        transition: all 0.3s ease;
    }
    
    .metric-card:hover {
        box-shadow: 0 4px 16px rgba(0,0,0,0.12);
        transform: translateY(-2px);
    }
    
    .savings-card {
        background: linear-gradient(135deg, #2e7d32 0%, #1b5e20 100%);
        color: white;
        padding: 28px;
        border-radius: 8px;
        text-align: center;
        box-shadow: 0 4px 12px rgba(46, 125, 50, 0.3);
    }
    
    .critical { color: #d32f2f; font-weight: 700; }
    .high { color: #f57c00; font-weight: 700; }
    .medium { color: #fbc02d; font-weight: 600; }
    .low { color: #2e7d32; font-weight: 600; }
    
    .kpi-title {
        font-size: 0.85em;
        text-transform: uppercase;
        letter-spacing: 1px;
        color: #666;
        font-weight: 600;
        margin-bottom: 8px;
    }
    
    .kpi-value {
        font-size: 2.2em;
        font-weight: 700;
        color: var(--primary);
        margin: 12px 0;
    }
    
    .section-header {
        border-bottom: 3px solid var(--primary);
        padding-bottom: 12px;
        margin-bottom: 20px;
        font-size: 1.6em;
        font-weight: 700;
    }
    
    .status-badge {
        display: inline-block;
        padding: 8px 16px;
        border-radius: 20px;
        font-size: 0.9em;
        font-weight: 600;
    }
    
    .status-running { background: #e3f2fd; color: #0288d1; }
    .status-success { background: #e8f5e9; color: #2e7d32; }
    .status-error { background: #ffebee; color: #d32f2f; }
    .status-idle { background: #f5f5f5; color: #666; }
    
    .insight-box {
        background: linear-gradient(135deg, #e3f2fd 0%, #f3e5f5 100%);
        padding: 16px;
        border-radius: 8px;
        margin: 12px 0;
        border-left: 4px solid var(--info);
    }
    
    .run-button {
        background: linear-gradient(135deg, #2e7d32 0%, #1b5e20 100%);
        color: white;
        padding: 12px 24px;
        border: none;
        border-radius: 6px;
        font-weight: 700;
        cursor: pointer;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# FILE PATH MANAGEMENT
# ============================================================================

def get_project_root():
    """Detect project root directory (your_submission)"""
    current = Path(__file__).parent
    while current != current.parent:
        if (current / "data").exists() and (current / "output").exists() and (current / "src").exists():
            return current
        current = current.parent
    # Fallback to current directory
    return Path.cwd()

PROJECT_ROOT = get_project_root()
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"
SRC_DIR = PROJECT_ROOT / "src"

# Create output directory if it doesn't exist
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

# ============================================================================
# SESSION STATE MANAGEMENT
# ============================================================================

if 'last_update' not in st.session_state:
    st.session_state.last_update = None

if 'program_output' not in st.session_state:
    st.session_state.program_output = []

if 'is_running' not in st.session_state:
    st.session_state.is_running = False

if 'auto_refresh' not in st.session_state:
    st.session_state.auto_refresh = True

if 'refresh_interval' not in st.session_state:
    st.session_state.refresh_interval = 5  # seconds

# ============================================================================
# DATA LOADING & CACHING
# ============================================================================

def get_report_modification_time(report_path):
    """Get last modification time of a report file"""
    try:
        return os.path.getmtime(report_path)
    except:
        return None

def load_all_reports():
    """Load all JSON reports with auto-refresh detection"""
    reports = {
        'anomaly': None,
        'accuracy': None,
        'llm_usage': None,
        'summary': None,
        'last_modified': None
    }
    
    try:
        anomaly_path = OUTPUT_DIR / 'anomaly_report.json'
        accuracy_path = OUTPUT_DIR / 'accuracy_report.json'
        llm_path = OUTPUT_DIR / 'llm_usage_report.json'
        
        # Load reports
        if anomaly_path.exists():
            with open(anomaly_path) as f:
                reports['anomaly'] = json.load(f)
        
        if accuracy_path.exists():
            with open(accuracy_path) as f:
                reports['accuracy'] = json.load(f)
        
        if llm_path.exists():
            with open(llm_path) as f:
                reports['llm_usage'] = json.load(f)
        
        # Get latest modification time
        mod_times = [
            get_report_modification_time(p) 
            for p in [anomaly_path, accuracy_path, llm_path] 
            if p.exists()
        ]
        if mod_times:
            reports['last_modified'] = max(mod_times)
        
        return reports
    except Exception as e:
        st.warning(f"Error loading reports: {e}")
        return reports

def has_new_output():
    """Check if output files have been updated"""
    reports = load_all_reports()
    current_time = reports.get('last_modified')
    
    if current_time is None:
        return False
    
    if st.session_state.last_update is None:
        st.session_state.last_update = current_time
        return False
    
    return current_time > st.session_state.last_update

def calculate_cost_savings(anomalies):
    """Calculate financial impact of detected anomalies"""
    savings = {
        'direct_savings': 0,
        'risk_avoidance': 0,
        'avoided_penalties': 0,
        'prevented_disputes': 0,
        'insurance_optimization': 0
    }
    
    for anom in anomalies:
        evidence = anom.get('evidence', {})
        
        if anom['anomaly_type'] == 'PRICE_MISMATCH':
            savings['direct_savings'] += abs(evidence.get('discrepancy', 0))
        
        elif anom['anomaly_type'] == 'INCOTERM_FREIGHT_MISMATCH':
            savings['prevented_disputes'] += 500
        
        elif anom['anomaly_type'] == 'INCOTERM_EXW_ERROR':
            savings['direct_savings'] += evidence.get('freight_cost', 0)
        
        elif anom['anomaly_type'] == 'INVALID_DRAWBACK_CLAIM':
            savings['avoided_penalties'] += evidence.get('drawback_claimed', 0) * 1.5
        
        elif anom['anomaly_type'] == 'EXCESSIVE_INSURANCE':
            savings['insurance_optimization'] += float(evidence.get('total_fob', 0) * 0.03)
        
        elif anom['anomaly_type'] == 'INVALID_HS_CODE_FORMAT':
            savings['prevented_disputes'] += 2500
        
        elif anom['anomaly_type'] == 'PRICE_OUTLIER':
            savings['direct_savings'] += abs(float(
                evidence.get('unit_price', 0) - evidence.get('standard_price', 0)
            ) * 2430)
        
        elif anom['anomaly_type'] == 'TRANSIT_TIME_OUTLIER':
            savings['prevented_disputes'] += 1000
        
        elif anom['anomaly_type'] == 'PAYMENT_BEHAVIOR_DETERIORATION':
            savings['risk_avoidance'] += 5000
    
    return savings

def find_python_files():
    """Find executable Python files in src directory"""
    if not SRC_DIR.exists():
        return {}
    
    python_files = {}
    for py_file in SRC_DIR.glob('*.py'):
        # Exclude __pycache__ and other special files
        if py_file.name.startswith('__'):
            continue
        python_files[py_file.name] = py_file
    
    return python_files

def run_program(program_path, output_placeholder):
    """Execute a Python program and stream output"""
    try:
        output_placeholder.info(f"🔄 Running {Path(program_path).name}...")
        st.session_state.is_running = True
        
        process = subprocess.Popen(
            ['python', str(program_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            bufsize=1,
            cwd=PROJECT_ROOT
        )
        
        output_lines = []
        
        # Stream output in real-time
        for line in process.stdout:
            line = line.strip()
            if line:
                output_lines.append(line)
                output_placeholder.info(f"📝 {line}")
        
        # Wait for process to complete
        process.wait()
        
        if process.returncode == 0:
            output_placeholder.success(f"✅ {Path(program_path).name} completed successfully!")
            st.session_state.program_output = output_lines
            
            # Check for new output files and trigger refresh
            time.sleep(1)
            st.session_state.last_update = datetime.now().timestamp()
            st.rerun()
        else:
            stderr = process.stderr.read()
            output_placeholder.error(f"❌ Error running program:\n{stderr}")
            st.session_state.program_output = output_lines
    
    except Exception as e:
        output_placeholder.error(f"❌ Failed to run program: {str(e)}")
    
    finally:
        st.session_state.is_running = False

# ============================================================================
# MAIN APPLICATION
# ============================================================================

def main():
    # Load reports
    reports = load_all_reports()
    anomaly_report = reports['anomaly'] or {'anomalies': [], 'metadata': {}}
    accuracy_report = reports['accuracy'] or {}
    llm_report = reports['llm_usage'] or {}
    
    anomalies = anomaly_report.get('anomalies', [])
    metadata = anomaly_report.get('metadata', {})
    
    # ========================================================================
    # SIDEBAR: CONTROL PANEL
    # ========================================================================
    
    with st.sidebar:
        st.markdown("## ⚙️ Control Panel")
        
        # Program execution section
        st.markdown("---")
        st.markdown("### 🚀 Run Analysis")
        
        python_files = find_python_files()
        
        if python_files:
            selected_program = st.selectbox(
                "Select program to run",
                list(python_files.keys()),
                help="Choose which analysis program to execute"
            )
            
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("▶️ Run", key="run_btn", use_container_width=True):
                    if not st.session_state.is_running:
                        program_path = python_files[selected_program]
                        output_container = st.container()
                        run_program(program_path, output_container)
                    else:
                        st.warning("⏳ A program is already running...")
            
            with col2:
                auto_refresh_enabled = st.toggle(
                    "Auto Refresh",
                    value=st.session_state.auto_refresh,
                    help="Automatically reload when new data is available"
                )
                st.session_state.auto_refresh = auto_refresh_enabled
            
            # Status indicator
            if st.session_state.is_running:
                st.markdown('<div class="status-badge status-running">🔄 Running...</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="status-badge status-idle">⏸️ Ready</div>', unsafe_allow_html=True)
        else:
            st.warning("⚠️ No Python files found in src/ directory")
        
        # Refresh settings
        st.markdown("---")
        st.markdown("### 🔄 Refresh Settings")
        
        st.session_state.refresh_interval = st.slider(
            "Refresh interval (seconds)",
            min_value=1,
            max_value=60,
            value=st.session_state.refresh_interval,
            help="How often to check for new output files"
        )
        
        if st.button("🔃 Refresh Now", use_container_width=True):
            st.rerun()
        
        # Data info
        st.markdown("---")
        st.markdown("### 📁 Project Structure")
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Data Files", len(list(DATA_DIR.glob('*'))) if DATA_DIR.exists() else 0)
        with col2:
            st.metric("Output Files", len(list(OUTPUT_DIR.glob('*.json'))))
        
        # Last update info
        if metadata.get('generated_at'):
            st.markdown("---")
            st.info(f"📅 Last analysis: {metadata.get('generated_at', 'N/A')[:10]}")
        
        # Auto-refresh timer
        if st.session_state.auto_refresh and has_new_output():
            st.markdown("---")
            st.success("✅ New data detected! Refreshing...", icon="✅")
            time.sleep(1)
            st.rerun()
    
    # ========================================================================
    # MAIN CONTENT: HEADER
    # ========================================================================
    
    st.markdown("""
    <div style='text-align: center; margin-bottom: 30px;'>
        <h1 style='margin: 0; color: #0f4c75;'>🚢 Shipment Anomaly Detection</h1>
        <p style='color: #666; font-size: 1.1em; margin-top: 8px;'>
            Real-Time AI-Powered Compliance & Risk Management
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Auto-refresh notification
    if st.session_state.auto_refresh:
        col1, col2 = st.columns([3, 1]
        )
        with col1:
            st.markdown("🔄 **Auto-refresh enabled** - Dashboard updates when new analysis completes")
        with col2:
            if st.button("Clear"):
                st.session_state.last_update = None
    
    # ========================================================================
    # SECTION 1: BUSINESS IMPACT & COST SAVINGS
    # ========================================================================
    
    if anomalies:
        st.markdown("<div class='section-header'>💰 Business Impact</div>", unsafe_allow_html=True)
        
        savings = calculate_cost_savings(anomalies)
        total_savings = sum(savings.values())
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
            <div class='savings-card'>
                <div class='kpi-title'>Total Value Protected</div>
                <div style='font-size: 2.5em; font-weight: 700;'>₹{total_savings:,.0f}</div>
                <div style='font-size: 0.9em; margin-top: 8px; opacity: 0.9;'>
                    Through anomaly detection
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class='metric-card'>
                <div class='kpi-title'>Direct Savings</div>
                <div class='kpi-value'>₹{savings['direct_savings']:,.0f}</div>
                <div style='color: #666; font-size: 0.9em;'>
                    Invoice corrections & cost avoidance
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class='metric-card'>
                <div class='kpi-title'>Risk Mitigation</div>
                <div class='kpi-value'>₹{savings['avoided_penalties'] + savings['prevented_disputes']:,.0f}</div>
                <div style='color: #666; font-size: 0.9em;'>
                    Penalties & disputes prevented
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div class='metric-card'>
                <div class='kpi-title'>Operational Efficiency</div>
                <div class='kpi-value'>₹{savings['risk_avoidance'] + savings['insurance_optimization']:,.0f}</div>
                <div style='color: #666; font-size: 0.9em;'>
                    Insurance & credit optimization
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        # Cost savings breakdown
        st.markdown("---")
        st.markdown("<div class='section-header'>📊 Cost Savings Breakdown</div>", unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            savings_data = {k.replace('_', ' ').title(): v for k, v in savings.items() if v > 0}
            if savings_data:
                df_savings = pd.DataFrame([
                    {'Category': k, 'Amount': v} for k, v in savings_data.items()
                ])
                fig = px.pie(
                    df_savings,
                    names='Category',
                    values='Amount',
                    title='Savings Distribution by Category',
                    color_discrete_sequence=['#2e7d32', '#0288d1', '#f57c00', '#d32f2f', '#1976d2']
                )
                fig.update_traces(textposition='inside', textinfo='label+percent')
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            top_savings = sorted(savings_data.items(), key=lambda x: x[1], reverse=True)
            df_top = pd.DataFrame([
                {'Savings Type': k.replace('_', ' ').title(), 'Amount (₹)': v} 
                for k, v in top_savings
            ])
            fig = px.bar(
                df_top,
                x='Savings Type',
                y='Amount (₹)',
                color='Amount (₹)',
                color_continuous_scale='Greens',
                title='Top Savings Opportunities'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # ========================================================================
    # SECTION 2: DETECTION METRICS & ACCURACY
    # ========================================================================
    
    st.markdown("---")
    st.markdown("<div class='section-header'>🎯 Detection Performance</div>", unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    if accuracy_report:
        metrics = accuracy_report.get('metrics', {})
        perf = accuracy_report.get('performance', {})
        
        with col1:
            st.markdown(f"""
            <div class='metric-card'>
                <div class='kpi-title'>Precision</div>
                <div class='kpi-value'>{metrics.get('precision', 0)*100:.1f}%</div>
                <div style='color: #666; font-size: 0.9em;'>
                    No false positives
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class='metric-card'>
                <div class='kpi-title'>Recall</div>
                <div class='kpi-value'>{metrics.get('recall', 0)*100:.1f}%</div>
                <div style='color: #666; font-size: 0.9em;'>
                    Anomalies caught
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class='metric-card'>
                <div class='kpi-title'>F1 Score</div>
                <div class='kpi-value'>{metrics.get('f1_score', 0):.3f}</div>
                <div style='color: #666; font-size: 0.9em;'>
                    Overall accuracy
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div class='metric-card'>
                <div class='kpi-title'>Accuracy</div>
                <div class='kpi-value'>{metrics.get('accuracy', 0)*100:.1f}%</div>
                <div style='color: #666; font-size: 0.9em;'>
                    Total correctness
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        # Detection accuracy visualization
        col1, col2 = st.columns(2)
        
        with col1:
            detection_data = {
                'Correctly Detected': accuracy_report.get('performance', {}).get('detected_correctly', 0),
                'Missed': accuracy_report.get('performance', {}).get('missed', 0),
                'False Positives': accuracy_report.get('performance', {}).get('false_positives', 0)
            }
            df_detection = pd.DataFrame([
                {'Type': k, 'Count': v} for k, v in detection_data.items() if v >= 0
            ])
            
            if len(df_detection) > 0:
                fig = px.bar(
                    df_detection,
                    x='Type',
                    y='Count',
                    color='Type',
                    color_discrete_map={
                        'Correctly Detected': '#2e7d32',
                        'Missed': '#f57c00',
                        'False Positives': '#d32f2f'
                    },
                    title='Detection Accuracy Results'
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            metadata_acc = accuracy_report.get('metadata', {})
            st.markdown(f"""
            <div class='insight-box'>
                <h3>📈 Accuracy Summary</h3>
                <p><strong>Planted Anomalies:</strong> {metadata_acc.get('total_planted_anomalies', 0)}</p>
                <p><strong>Detected:</strong> {metadata_acc.get('total_detected_anomalies', 0)}</p>
                <p><strong>Correctly Identified:</strong> {metadata_acc.get('correctly_detected', 0)}</p>
                <p><strong>Detection Rate:</strong> {metadata_acc.get('correctly_detected', 0)}/{metadata_acc.get('total_planted_anomalies', 1)}</p>
            </div>
            """, unsafe_allow_html=True)
    
    # ========================================================================
    # SECTION 3: ANOMALY OVERVIEW
    # ========================================================================
    
    st.markdown("---")
    st.markdown("<div class='section-header'>🚨 Anomaly Overview</div>", unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    critical_count = len([a for a in anomalies if a.get('severity') == 'CRITICAL'])
    high_count = len([a for a in anomalies if a.get('severity') == 'HIGH'])
    medium_count = len([a for a in anomalies if a.get('severity') == 'MEDIUM'])
    low_count = len([a for a in anomalies if a.get('severity') == 'LOW'])
    
    with col1:
        st.markdown(f"""
        <div class='metric-card' style='border-left-color: #d32f2f;'>
            <div class='kpi-title'>🔴 Critical</div>
            <div class='kpi-value' style='color: #d32f2f;'>{critical_count}</div>
            <div style='color: #666; font-size: 0.9em;'>Immediate action required</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class='metric-card' style='border-left-color: #f57c00;'>
            <div class='kpi-title'>🟠 High</div>
            <div class='kpi-value' style='color: #f57c00;'>{high_count}</div>
            <div style='color: #666; font-size: 0.9em;'>Review recommended</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class='metric-card' style='border-left-color: #fbc02d;'>
            <div class='kpi-title'>🟡 Medium</div>
            <div class='kpi-value' style='color: #fbc02d;'>{medium_count}</div>
            <div style='color: #666; font-size: 0.9em;'>Monitor closely</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class='metric-card' style='border-left-color: #2e7d32;'>
            <div class='kpi-title'>🟢 Low</div>
            <div class='kpi-value' style='color: #2e7d32;'>{low_count}</div>
            <div style='color: #666; font-size: 0.9em;'>Optimization opportunity</div>
        </div>
        """, unsafe_allow_html=True)
    
    # Severity distribution
    col1, col2 = st.columns(2)
    
    with col1:
        severity_data = {
            'CRITICAL': critical_count,
            'HIGH': high_count,
            'MEDIUM': medium_count,
            'LOW': low_count
        }
        df_sev = pd.DataFrame([
            {'Severity': k, 'Count': v} for k, v in severity_data.items()
        ])
        
        fig = px.bar(
            df_sev,
            x='Severity',
            y='Count',
            color='Severity',
            color_discrete_map={
                'CRITICAL': '#d32f2f',
                'HIGH': '#f57c00',
                'MEDIUM': '#fbc02d',
                'LOW': '#2e7d32'
            },
            title='Anomalies by Severity Level',
            category_orders={'Severity': ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        layer_data = {}
        for a in anomalies:
            layer = f"Layer {a.get('layer', '?')}"
            layer_data[layer] = layer_data.get(layer, 0) + 1
        
        df_layer = pd.DataFrame([
            {'Layer': k, 'Count': v} for k, v in layer_data.items()
        ])
        
        fig = px.pie(
            df_layer,
            names='Layer',
            values='Count',
            title='Anomalies by Detection Layer',
            color_discrete_sequence=['#0f4c75', '#0288d1', '#4fc3f7']
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # ========================================================================
    # SECTION 4: ANOMALY TYPES ANALYSIS
    # ========================================================================
    
    st.markdown("---")
    st.markdown("<div class='section-header'>📋 Anomaly Types</div>", unsafe_allow_html=True)
    
    anomaly_types = {}
    for a in anomalies:
        atype = a.get('anomaly_type', 'Unknown')
        anomaly_types[atype] = anomaly_types.get(atype, 0) + 1
    
    df_types = pd.DataFrame([
        {'Anomaly Type': k, 'Count': v} 
        for k, v in sorted(anomaly_types.items(), key=lambda x: x[1], reverse=True)
    ])
    
    fig = px.bar(
        df_types,
        x='Count',
        y='Anomaly Type',
        orientation= "h",
        color='Count',
        color_continuous_scale='Reds',
        title='Most Common Anomaly Types'
    )
    fig.update_layout(height=400, yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig, use_container_width=True)
    
    # ========================================================================
    # SECTION 5: CRITICAL ANOMALIES TABLE
    # ========================================================================
    
    st.markdown("---")
    st.markdown("<div class='section-header'>⚠️ Critical & High Priority Anomalies</div>", unsafe_allow_html=True)
    
    critical_high = [a for a in anomalies if a.get('severity') in ['CRITICAL', 'HIGH']]
    
    if critical_high:
        df_critical = pd.DataFrame([
            {
                'ID': a.get('shipment_id', a.get('buyer_id', 'N/A')),
                'Type': a['anomaly_type'],
                'Layer': f"L{a['layer']}",
                'Severity': a['severity'],
                'Impact': a.get('impact', 'N/A')[:60] + '...',
                'Action': a.get('recommendation', 'N/A')[:50] + '...'
            }
            for a in critical_high
        ])
        
        st.dataframe(
            df_critical,
            use_container_width=True,
            hide_index=True,
            height=min(500, len(df_critical) * 35 + 50)
        )
    else:
        st.success("✅ No critical or high severity anomalies detected!")
    
    # ========================================================================
    # SECTION 6: DETAILED ANOMALY EXPLORER
    # ========================================================================
    
    st.markdown("---")
    st.markdown("<div class='section-header'>🔍 Anomaly Details Explorer</div>", unsafe_allow_html=True)
    
    if anomalies:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            filter_severity = st.multiselect(
                "Filter by Severity",
                ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW'],
                default=['CRITICAL', 'HIGH'],
                key='severity_filter'
            )
        
        with col2:
            filter_layer = st.multiselect(
                "Filter by Layer",
                [1, 2, 3],
                default=[1, 2, 3],
                key='layer_filter'
            )
        
        with col3:
            filter_type = st.multiselect(
                "Filter by Type",
                list(anomaly_types.keys()),
                default=list(anomaly_types.keys())[:3],
                key='type_filter'
            )
        
        filtered = [
            a for a in anomalies
            if (a.get('severity', 'MEDIUM') in filter_severity) and
               (a.get('layer', 2) in filter_layer) and
               (a.get('anomaly_type', '') in filter_type)
        ]
        
        if filtered:
            st.info(f"Showing {len(filtered)} of {len(anomalies)} anomalies")
            
            selected_idx = st.selectbox(
                "Select anomaly to view details",
                range(len(filtered)),
                format_func=lambda i: f"[{filtered[i]['severity']}] {filtered[i]['anomaly_type']} - ID: {filtered[i].get('shipment_id', filtered[i].get('buyer_id', 'N/A'))}",
                key='anomaly_select'
            )
            
            anomaly = filtered[selected_idx]
            
            col1, col2, col3 = st.columns([2, 2, 2])
            
            with col1:
                st.markdown(f"**Type:** {anomaly['anomaly_type']}")
                st.markdown(f"**Layer:** {anomaly['layer']}")
            
            with col2:
                st.markdown(f"**Severity:** <span class='{anomaly['severity'].lower()}'>{anomaly['severity']}</span>", unsafe_allow_html=True)
                st.markdown(f"**ID:** {anomaly.get('shipment_id', anomaly.get('buyer_id', 'N/A'))}")
            
            with col3:
                st.markdown(f"**Risk Score:** {anomaly.get('risk_score', 'N/A')}")
            
            tab1, tab2, tab3 = st.tabs(["Impact & Action", "Evidence", "Full Details"])
            
            with tab1:
                st.subheader("💥 Impact")
                st.warning(anomaly.get('impact', 'No impact information'))
                
                st.subheader("✅ Recommended Action")
                st.info(anomaly.get('recommendation', 'No recommendation'))
            
            with tab2:
                st.subheader("📊 Evidence")
                evidence = anomaly.get('evidence', {})
                
                if isinstance(evidence, dict):
                    for key, value in evidence.items():
                        if isinstance(value, dict):
                            st.json(value)
                        else:
                            st.write(f"**{key}:** `{value}`")
                else:
                    st.write(evidence)
            
            with tab3:
                st.json(anomaly)
        else:
            st.warning("No anomalies match your filter criteria")
    
    # ========================================================================
    # SECTION 7: LLM USAGE & COSTS
    # ========================================================================
    
    st.markdown("---")
    st.markdown("<div class='section-header'>⚙️ System Efficiency & LLM Usage</div>", unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    if llm_report:
        with col1:
            st.markdown(f"""
            <div class='metric-card'>
                <div class='kpi-title'>LLM Calls</div>
                <div class='kpi-value'>{llm_report.get('total_calls', 0)}</div>
                <div style='color: #666; font-size: 0.9em;'>
                    {llm_report.get('provider', 'N/A')} - {llm_report.get('model', 'N/A')}
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            total_tokens = llm_report.get('total_tokens', {}).get('total', 0)
            st.markdown(f"""
            <div class='metric-card'>
                <div class='kpi-title'>Tokens Used</div>
                <div class='kpi-value'>{total_tokens:,}</div>
                <div style='color: #666; font-size: 0.9em;'>
                    Input + Output
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            cost = llm_report.get('estimated_cost_usd', 0)
            st.markdown(f"""
            <div class='metric-card'>
                <div class='kpi-title'>Estimated Cost</div>
                <div class='kpi-value'>${cost:.4f}</div>
                <div style='color: #666; font-size: 0.9em;'>
                    USD equivalent
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            total_savings = sum(calculate_cost_savings(anomalies).values()) if anomalies else 0
            roi = total_savings / max(cost, 1) if cost > 0 else float('inf')
            st.markdown(f"""
            <div class='metric-card'>
                <div class='kpi-title'>ROI Multiple</div>
                <div class='kpi-value' style='color: #2e7d32;'>{roi:,.0f}x</div>
                <div style='color: #666; font-size: 0.9em;'>
                    Value per dollar spent
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    # ========================================================================
    # SECTION 8: EXECUTION METADATA
    # ========================================================================
    
    st.markdown("---")
    st.markdown("<div class='section-header'>📈 Analysis Metadata</div>", unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class='metric-card'>
            <div class='kpi-title'>Execution Time</div>
            <div class='kpi-value'>{metadata.get('execution_time_seconds', 0):.3f}s</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class='metric-card'>
            <div class='kpi-title'>Total Anomalies</div>
            <div class='kpi-value'>{len(anomalies)}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class='metric-card'>
            <div class='kpi-title'>Generated</div>
            <div class='kpi-value'>{metadata.get('generated_at', 'N/A')[:10]}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class='metric-card'>
            <div class='kpi-title'>Detection Layers</div>
            <div class='kpi-value'>3</div>
            <div style='color: #666; font-size: 0.9em;'>Rules, Stats, LLM</div>
        </div>
        """, unsafe_allow_html=True)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #888; font-size: 0.85em; padding: 20px;'>
        <p><strong>🚀 Shipment Anomaly Detection System</strong></p>
        <p>3-Layer Detection Pipeline: Rule-Based + Statistical + LLM-Powered</p>
        <p style='margin-top: 10px; color: #aaa;'>
            Built with Streamlit, Pandas, Plotly | Real-time Compliance & Risk Management
        </p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()