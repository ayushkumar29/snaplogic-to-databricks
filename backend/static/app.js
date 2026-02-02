/**
 * SnapLogic to Databricks Converter - Frontend Application
 */

// ==================== State Management ====================
const state = {
    pipelines: [],
    selectedPipeline: null,
    generatedCode: '',
    missingDependencies: [],
    settings: {
        llmProvider: 'openai',
        apiKey: '',
        autoOptimize: true
    }
};

// ==================== DOM Elements ====================
const elements = {
    uploadZone: document.getElementById('uploadZone'),
    fileInput: document.getElementById('fileInput'),
    pipelineList: document.getElementById('pipelineList'),
    missingDeps: document.getElementById('missingDeps'),
    missingDepsList: document.getElementById('missingDepsList'),
    convertBtn: document.getElementById('convertBtn'),
    codeOutput: document.getElementById('codeOutput'),
    chatMessages: document.getElementById('chatMessages'),
    chatInput: document.getElementById('chatInput'),
    settingsModal: document.getElementById('settingsModal'),
    loadingOverlay: document.getElementById('loadingOverlay'),
    loadingText: document.getElementById('loadingText'),
    toastContainer: document.getElementById('toastContainer'),
    aiStatus: document.getElementById('aiStatus')
};

// ==================== Initialization ====================
document.addEventListener('DOMContentLoaded', () => {
    initializeApp();
    loadSettings();
    checkAIStatus();
});

function initializeApp() {
    // Setup file upload
    setupUploadZone();
    
    // Setup tabs
    setupTabs();
    
    // Setup chat input
    elements.chatInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            sendMessage();
        }
    });
    
    // Initialize syntax highlighting
    hljs.highlightAll();
}

// ==================== File Upload ====================
function setupUploadZone() {
    const zone = elements.uploadZone;
    const input = elements.fileInput;
    
    // Click to upload
    zone.addEventListener('click', () => input.click());
    
    // File input change
    input.addEventListener('change', handleFiles);
    
    // Drag and drop
    zone.addEventListener('dragover', (e) => {
        e.preventDefault();
        zone.classList.add('drag-over');
    });
    
    zone.addEventListener('dragleave', () => {
        zone.classList.remove('drag-over');
    });
    
    zone.addEventListener('drop', (e) => {
        e.preventDefault();
        zone.classList.remove('drag-over');
        
        const files = e.dataTransfer.files;
        if (files.length) {
            uploadFiles(files);
        }
    });
}

function handleFiles(e) {
    const files = e.target.files;
    if (files.length) {
        uploadFiles(files);
    }
}

async function uploadFiles(files) {
    showLoading('Uploading pipelines...');
    
    const formData = new FormData();
    for (const file of files) {
        formData.append('files', file);
    }
    
    try {
        const response = await fetch('/api/upload', {
            method: 'POST',
            body: formData
        });
        
        const data = await response.json();
        
        if (data.success) {
            // Add to state
            state.pipelines = [...state.pipelines, ...data.pipelines];
            state.missingDependencies = data.missing_dependencies || [];
            
            // Update UI
            renderPipelineList();
            updateMissingDeps();
            updateConvertButton();
            
            showToast(`Uploaded ${data.pipelines.length} pipeline(s)`, 'success');
        } else {
            showToast('Upload failed: ' + (data.error || 'Unknown error'), 'error');
        }
    } catch (error) {
        console.error('Upload error:', error);
        showToast('Upload failed: ' + error.message, 'error');
    } finally {
        hideLoading();
        elements.fileInput.value = '';
    }
}

// ==================== Pipeline List ====================
function renderPipelineList() {
    if (state.pipelines.length === 0) {
        elements.pipelineList.innerHTML = `
            <div class="empty-state">
                <p>No pipelines uploaded yet</p>
            </div>
        `;
        return;
    }
    
    elements.pipelineList.innerHTML = state.pipelines.map((pipeline, index) => `
        <div class="pipeline-item ${state.selectedPipeline === index ? 'selected' : ''}" 
             onclick="selectPipeline(${index})"
             data-index="${index}">
            <div class="pipeline-icon">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="16" height="16">
                    <polyline points="16 18 22 12 16 6"/>
                    <polyline points="8 6 2 12 8 18"/>
                </svg>
            </div>
            <div class="pipeline-info">
                <div class="pipeline-name">${escapeHtml(pipeline.pipeline_name || pipeline.filename)}</div>
                <div class="pipeline-meta">${pipeline.snap_count || 0} snaps</div>
            </div>
            ${pipeline.error ? '<span class="badge badge-error">Error</span>' : ''}
        </div>
    `).join('');
}

function selectPipeline(index) {
    state.selectedPipeline = index;
    renderPipelineList();
}

function updateMissingDeps() {
    if (state.missingDependencies.length > 0) {
        elements.missingDeps.style.display = 'block';
        elements.missingDepsList.textContent = state.missingDependencies.join(', ');
    } else {
        elements.missingDeps.style.display = 'none';
    }
}

function updateConvertButton() {
    elements.convertBtn.disabled = state.pipelines.length === 0;
}

function clearAll() {
    state.pipelines = [];
    state.selectedPipeline = null;
    state.generatedCode = '';
    state.missingDependencies = [];
    
    renderPipelineList();
    updateMissingDeps();
    updateConvertButton();
    
    elements.codeOutput.textContent = `# Your converted Databricks code will appear here
# 
# 1. Upload one or more SnapLogic pipeline (.slp) files
# 2. Click "Convert to Databricks"
# 3. View and download your PySpark code

print("Ready to convert!")`;
    
    hljs.highlightElement(elements.codeOutput);
    
    showToast('All pipelines cleared', 'info');
}

// ==================== Conversion ====================
async function convertPipelines() {
    if (state.pipelines.length === 0) {
        showToast('Please upload at least one pipeline', 'warning');
        return;
    }
    
    showLoading('Converting pipelines to PySpark...');
    
    const pipelineNames = state.pipelines.map(p => p.filename);
    
    try {
        const formData = new FormData();
        pipelineNames.forEach(name => formData.append('pipeline_names', name));
        formData.append('use_ai', state.settings.autoOptimize);
        
        const response = await fetch('/api/convert', {
            method: 'POST',
            body: formData
        });
        
        const data = await response.json();
        
        // Combine all generated code
        let combinedCode = '';
        for (const result of data.results) {
            if (result.success) {
                combinedCode += result.code + '\n\n';
            } else {
                combinedCode += `# Error converting ${result.pipeline}: ${result.error}\n\n`;
            }
        }
        
        state.generatedCode = combinedCode;
        
        // Display code
        elements.codeOutput.textContent = combinedCode;
        hljs.highlightElement(elements.codeOutput);
        
        // Handle unknown snaps
        if (data.unknown_snaps && data.unknown_snaps.length > 0) {
            addChatMessage('system', `⚠️ Found ${data.unknown_snaps.length} unknown snap type(s). Would you like me to help convert them using AI?`);
        }
        
        showToast('Conversion complete!', 'success');
        
    } catch (error) {
        console.error('Conversion error:', error);
        showToast('Conversion failed: ' + error.message, 'error');
    } finally {
        hideLoading();
    }
}

// ==================== Code Actions ====================
function copyCode() {
    if (!state.generatedCode) {
        showToast('No code to copy', 'warning');
        return;
    }
    
    navigator.clipboard.writeText(state.generatedCode)
        .then(() => showToast('Code copied to clipboard', 'success'))
        .catch(() => showToast('Failed to copy code', 'error'));
}

function downloadCode() {
    if (!state.generatedCode) {
        showToast('No code to download', 'warning');
        return;
    }
    
    const blob = new Blob([state.generatedCode], { type: 'text/x-python' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'converted_pipeline.py';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    
    showToast('Code downloaded', 'success');
}

// ==================== Tabs ====================
function setupTabs() {
    document.querySelectorAll('.tab').forEach(tab => {
        tab.addEventListener('click', () => {
            const tabName = tab.dataset.tab;
            
            // Update tab buttons
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            
            // Update tab content
            document.querySelectorAll('.tab-content').forEach(content => {
                content.classList.remove('active');
            });
            document.getElementById(tabName + 'Tab').classList.add('active');
        });
    });
}

// ==================== AI Chat ====================
async function sendMessage() {
    const input = elements.chatInput;
    const message = input.value.trim();
    
    if (!message) return;
    
    // Add user message
    addChatMessage('user', message);
    input.value = '';
    
    // Show typing indicator
    const typingId = addChatMessage('assistant', '...');
    
    try {
        const response = await fetch('/api/ask-ai', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                snap_data: state.selectedPipeline !== null ? state.pipelines[state.selectedPipeline] : {},
                question: message
            })
        });
        
        const data = await response.json();
        
        // Update typing indicator with response
        updateChatMessage(typingId, data.response);
        
    } catch (error) {
        updateChatMessage(typingId, 'Sorry, I encountered an error: ' + error.message);
    }
}

function addChatMessage(type, content) {
    const id = 'msg-' + Date.now();
    const messageEl = document.createElement('div');
    messageEl.id = id;
    messageEl.className = `message ${type}`;
    messageEl.innerHTML = formatMessage(content);
    elements.chatMessages.appendChild(messageEl);
    elements.chatMessages.scrollTop = elements.chatMessages.scrollHeight;
    return id;
}

function updateChatMessage(id, content) {
    const messageEl = document.getElementById(id);
    if (messageEl) {
        messageEl.innerHTML = formatMessage(content);
    }
}

function formatMessage(content) {
    // Convert markdown-like formatting
    return content
        .replace(/```(\w*)\n([\s\S]*?)```/g, '<pre><code class="language-$1">$2</code></pre>')
        .replace(/`([^`]+)`/g, '<code>$1</code>')
        .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
        .replace(/\n/g, '<br>');
}

// ==================== Settings ====================
function openSettings() {
    elements.settingsModal.classList.add('open');
    
    // Populate current settings
    document.getElementById('llmProvider').value = state.settings.llmProvider;
    document.getElementById('apiKey').value = state.settings.apiKey;
    document.getElementById('autoOptimize').checked = state.settings.autoOptimize;
}

function closeSettings() {
    elements.settingsModal.classList.remove('open');
}

function saveSettings() {
    state.settings.llmProvider = document.getElementById('llmProvider').value;
    state.settings.apiKey = document.getElementById('apiKey').value;
    state.settings.autoOptimize = document.getElementById('autoOptimize').checked;
    
    // Save to localStorage
    localStorage.setItem('converterSettings', JSON.stringify(state.settings));
    
    closeSettings();
    showToast('Settings saved', 'success');
    checkAIStatus();
}

function loadSettings() {
    const saved = localStorage.getItem('converterSettings');
    if (saved) {
        try {
            state.settings = { ...state.settings, ...JSON.parse(saved) };
        } catch (e) {
            console.error('Failed to load settings:', e);
        }
    }
}

async function checkAIStatus() {
    const statusEl = elements.aiStatus;
    const dot = statusEl.querySelector('.status-dot');
    const text = statusEl.querySelector('span:last-child');
    
    if (state.settings.apiKey) {
        dot.style.background = '#10b981';
        text.textContent = 'AI Ready';
    } else {
        dot.style.background = '#f59e0b';
        text.textContent = 'No API Key';
    }
}

// ==================== UI Helpers ====================
function showLoading(message = 'Loading...') {
    elements.loadingText.textContent = message;
    elements.loadingOverlay.classList.add('active');
}

function hideLoading() {
    elements.loadingOverlay.classList.remove('active');
}

function showToast(message, type = 'info') {
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.textContent = message;
    
    elements.toastContainer.appendChild(toast);
    
    // Auto remove after 3 seconds
    setTimeout(() => {
        toast.style.animation = 'toastIn 0.3s ease reverse';
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// ==================== Export for HTML onclick ====================
window.selectPipeline = selectPipeline;
window.clearAll = clearAll;
window.convertPipelines = convertPipelines;
window.copyCode = copyCode;
window.downloadCode = downloadCode;
window.openSettings = openSettings;
window.closeSettings = closeSettings;
window.saveSettings = saveSettings;
window.sendMessage = sendMessage;
