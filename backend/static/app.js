/**
 * SnapLogic to Databricks Converter - Frontend Application
 * Enhanced with credential detection and custom snap handling
 */

// ==================== State Management ====================
const state = {
    pipelines: [],
    selectedPipeline: null,
    generatedCode: '',
    missingDependencies: [],
    detectedCredentials: null,
    credentialValues: {},
    settings: {
        llmProvider: 'groq',
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
    credentialsModal: document.getElementById('credentialsModal'),
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
    showLoading('Uploading and analyzing pipelines...');

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
            state.detectedCredentials = data.detected_credentials || null;

            // Update UI
            renderPipelineList();
            updateMissingDeps();
            updateConvertButton();

            // Show credential detection results
            if (hasCredentials(data.detected_credentials)) {
                showToast(`Found credentials/accounts that need configuration`, 'warning');
                addChatMessage('system', `🔐 Detected ${countCredentials(data.detected_credentials)} credential(s)/account(s) in your pipeline. These will need Databricks equivalents.`);
            }

            // Show custom snap warning
            if (data.detected_credentials?.custom_snaps?.length > 0) {
                addChatMessage('system', `⚠️ Found ${data.detected_credentials.custom_snaps.length} custom/enterprise snap(s) that may require manual conversion.`);
            }

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

function hasCredentials(creds) {
    if (!creds) return false;
    return creds.accounts?.length > 0 ||
        creds.connections?.length > 0 ||
        creds.credentials?.length > 0 ||
        creds.paths?.length > 0;
}

function countCredentials(creds) {
    if (!creds) return 0;
    return (creds.accounts?.length || 0) +
        (creds.connections?.length || 0) +
        (creds.credentials?.length || 0) +
        (creds.paths?.length || 0);
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
                <div class="pipeline-meta">
                    ${pipeline.snap_count || 0} snaps
                    ${pipeline.has_custom_snaps ? '<span class="badge badge-warning">Custom</span>' : ''}
                    ${pipeline.requires_credentials ? '<span class="badge badge-info">Creds</span>' : ''}
                </div>
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
    state.detectedCredentials = null;
    state.credentialValues = {};

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

// ==================== Credentials Modal ====================
function openCredentialsModal() {
    if (!state.detectedCredentials) return;

    const creds = state.detectedCredentials;

    // Populate sections
    populateCredSection('accounts', creds.accounts, 'accountsList', 'accountsSection');
    populateCredSection('connections', creds.connections, 'connectionsList', 'connectionsSection');
    populateCredSection('credentials', creds.credentials, 'credentialsList', 'credentialsSection');
    populateCredSection('paths', creds.paths, 'pathsList', 'pathsSection');
    populateCustomSnapsSection(creds.custom_snaps);

    elements.credentialsModal.classList.add('open');
}

function populateCredSection(type, items, listId, sectionId) {
    const listEl = document.getElementById(listId);
    const sectionEl = document.getElementById(sectionId);

    if (!items || items.length === 0) {
        sectionEl.style.display = 'none';
        return;
    }

    sectionEl.style.display = 'block';
    listEl.innerHTML = items.map((item, i) => `
        <div class="cred-item">
            <div class="cred-info">
                <span class="cred-snap">${escapeHtml(item.snap_name)}</span>
                <span class="cred-property">${escapeHtml(item.property)}</span>
                ${item.current_value ? `<span class="cred-current">Current: ${escapeHtml(String(item.current_value))}</span>` : ''}
            </div>
            <input type="text" 
                   class="cred-input" 
                   placeholder="Databricks value or dbutils.secrets.get(...)"
                   data-type="${type}"
                   data-index="${i}"
                   data-property="${item.property}">
        </div>
    `).join('');
}

function populateCustomSnapsSection(items) {
    const listEl = document.getElementById('customSnapsList');
    const sectionEl = document.getElementById('customSnapsSection');

    if (!items || items.length === 0) {
        sectionEl.style.display = 'none';
        return;
    }

    sectionEl.style.display = 'block';
    listEl.innerHTML = items.map(item => `
        <div class="cred-item custom-snap-item">
            <div class="cred-info">
                <span class="cred-snap">${escapeHtml(item.snap_name)}</span>
                <span class="cred-property">${escapeHtml(item.snap_type)}</span>
            </div>
            <button class="btn btn-sm btn-primary" onclick="askAIAboutSnap('${escapeHtml(item.snap_name)}')">
                Ask AI
            </button>
        </div>
    `).join('');
}

function closeCredentialsModal() {
    elements.credentialsModal.classList.remove('open');
}

function applyCredentials() {
    // Collect all credential values
    const inputs = document.querySelectorAll('.cred-input');
    state.credentialValues = {};

    inputs.forEach(input => {
        if (input.value.trim()) {
            const property = input.dataset.property;
            state.credentialValues[property] = input.value.trim();
        }
    });

    closeCredentialsModal();
    doConvert();
}

async function askAIAboutSnap(snapName) {
    const pipeline = state.pipelines.find(p =>
        p.data?.snaps?.some(s => s.name === snapName)
    );

    if (pipeline) {
        const snap = pipeline.data.snaps.find(s => s.name === snapName);
        addChatMessage('user', `How do I convert this custom snap to Databricks? ${snapName}`);

        // Trigger AI response
        try {
            const response = await fetch('/api/analyze-custom-snap', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ snap_data: snap })
            });

            const data = await response.json();

            if (data.generated_code) {
                addChatMessage('assistant', `Here's how to convert **${snapName}**:\n\n\`\`\`python\n${data.generated_code}\n\`\`\`\n\n${data.suggested_approach || ''}`);
            } else if (data.questions_for_user?.length > 0) {
                const questions = data.questions_for_user.map(q => `• ${q.question}`).join('\n');
                addChatMessage('assistant', `I need more information to convert **${snapName}**:\n\n${questions}`);
            }
        } catch (error) {
            addChatMessage('assistant', `Error analyzing snap: ${error.message}`);
        }
    }

    closeCredentialsModal();
}

// ==================== Conversion ====================
async function convertPipelines() {
    if (state.pipelines.length === 0) {
        showToast('Please upload at least one pipeline', 'warning');
        return;
    }

    // Check if we have detected credentials that need configuration
    if (hasCredentials(state.detectedCredentials) && Object.keys(state.credentialValues).length === 0) {
        openCredentialsModal();
        return;
    }

    doConvert();
}

async function doConvert() {
    showLoading('Converting pipelines to PySpark...');

    const pipelineNames = state.pipelines.map(p => p.filename);

    try {
        const formData = new FormData();
        pipelineNames.forEach(name => formData.append('pipeline_names', name));
        formData.append('use_ai', state.settings.autoOptimize);

        // Add credential config if any
        if (Object.keys(state.credentialValues).length > 0) {
            const credConfig = Object.entries(state.credentialValues)
                .map(([k, v]) => `${k}=${v}`)
                .join('\n');
            formData.append('credential_config', credConfig);
        }

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

        // Handle custom snaps requiring input
        if (data.custom_snaps_requiring_input?.length > 0) {
            addChatMessage('system', `🔧 ${data.custom_snaps_requiring_input.length} custom snap(s) need additional context. Click "Ask AI" next to each for help.`);
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
                snap_data: state.selectedPipeline !== null ? state.pipelines[state.selectedPipeline]?.data : {},
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

    try {
        const response = await fetch('/api/llm-status');
        const data = await response.json();

        if (data.available) {
            dot.style.background = '#10b981';
            text.textContent = `AI: ${data.provider}`;
        } else {
            dot.style.background = '#f59e0b';
            text.textContent = 'AI: Not configured';
        }
    } catch {
        dot.style.background = '#ef4444';
        text.textContent = 'AI: Offline';
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
window.openCredentialsModal = openCredentialsModal;
window.closeCredentialsModal = closeCredentialsModal;
window.applyCredentials = applyCredentials;
window.askAIAboutSnap = askAIAboutSnap;
