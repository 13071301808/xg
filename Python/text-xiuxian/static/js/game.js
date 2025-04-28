// 修仙游戏相关的JavaScript功能
document.addEventListener('DOMContentLoaded', function() {
    // 自动更新修炼进度
    function updateCultivationProgress() {
        const progressBar = document.querySelector('.progress-bar-fill');
        if (progressBar) {
            const currentProgress = parseFloat(progressBar.getAttribute('data-progress'));
            progressBar.style.width = currentProgress + '%';
        }
    }

    // 定时刷新修炼状态
    function refreshCultivationStatus() {
        const cultivateButton = document.querySelector('#cultivate-btn');
        if (cultivateButton) {
            cultivateButton.click();
        }
    }

    // 初始化
    updateCultivationProgress();
    
    // 每60秒自动修炼一次
    setInterval(refreshCultivationStatus, 60000);
}); 