#!/usr/bin/env python3
"""
Stash Auto Scanner - Stash 插件版
支持 CD2 挂载网盘的轮询监控模式
"""

import os
import sys
import time
import json
import threading
from datetime import datetime
from pathlib import Path

# ==================== 全局状态 ====================
running = False
monitor_thread = None
pending_scans = set()
processing_lock = False
# =================================================

def parse_settings(args):
    """解析 Stash 传来的设置"""
    settings = {
        "enabled": True,
        "auto_start": True,
        "watch_paths": "/Volumes/115/9.Porns/2.Western\n/Volumes/115/9.Porns/1.Japan",
        "scan_delay": 30,
        "identify_delay": 120,
        "nfo_delay": 180,
        "use_polling": True,
        "poll_interval": 30,
        "exclude_dirs": [".tmp", ".temp", ".grab", ".stfolder", "trash", "temp", "tmp"],
        "video_extensions": [".mp4", ".mkv", ".avi", ".mov", ".wmv", ".m4v", ".flv", ".webm", ".ts", ".m2ts"],
    }
    
    if "enabled" in args:
        settings["enabled"] = str(args["enabled"]).lower() == "true"
    if "auto_start" in args:
        settings["auto_start"] = str(args["auto_start"]).lower() == "true"
    if "watch_paths" in args:
        paths = args["watch_paths"].strip().split("\n")
        settings["watch_paths"] = [p.strip() for p in paths if p.strip()]
    if "scan_delay" in args:
        settings["scan_delay"] = int(args["scan_delay"])
    if "identify_delay" in args:
        settings["identify_delay"] = int(args["identify_delay"])
    if "nfo_delay" in args:
        settings["nfo_delay"] = int(args["nfo_delay"])
    if "use_polling" in args:
        settings["use_polling"] = str(args["use_polling"]).lower() == "true"
    if "poll_interval" in args:
        settings["poll_interval"] = int(args["poll_interval"])
    
    return settings

def log_info(message):
    print(f"[INFO] {message}", flush=True)

def log_error(message):
    print(f"[ERROR] {message}", flush=True)

def log_warn(message):
    print(f"[WARN] {message}", flush=True)

def log_debug(message):
    print(f"[DEBUG] {message}", flush=True)

def graphql_request(stash_conn, query, variables=None):
    """发送 GraphQL 请求"""
    try:
        import requests
        
        url = stash_conn.get("url", "http://localhost:9999/graphql")
        api_key = stash_conn.get("api_key", "")
        
        headers = {'Content-Type': 'application/json'}
        if api_key:
            headers['ApiKey'] = api_key
        
        payload = {'query': query, 'variables': variables or {}}
        
        response = requests.post(url, headers=headers, json=payload, timeout=120)
        
        if response.status_code == 200:
            return response.json()
        else:
            log_error(f"GraphQL 错误: HTTP {response.status_code}")
            return None
    except Exception as e:
        log_error(f"GraphQL 异常: {e}")
        return None

def should_process_file(filepath, config):
    filepath_lower = filepath.lower()
    for exclude in config.get("exclude_dirs", []):
        if exclude.lower() in filepath_lower:
            return False
    ext = Path(filepath).suffix.lower()
    if ext not in config.get("video_extensions", []):
        return False
    return True

def scan_path(stash_conn, path):
    log_info(f"📁 开始 Scan: {path}")
    query = """
    mutation Scan($path: String!) {
        metadataScan(input: {
            paths: [$path]
            scanGenerateCovers: true
            scanGeneratePreviews: true
            scanGenerateSprites: true
        })
    }
    """
    result = graphql_request(stash_conn, query, {'path': path})
    if result and 'data' in result:
        log_info(f"✅ Scan 已触发")
        return True
    else:
        log_error(f"❌ Scan 失败")
        return False

def run_auto_identify(stash_conn, scene_ids):
    if not scene_ids:
        return False
    log_info(f"🔍 开始 Identify {len(scene_ids)} 个场景...")
    
    query = """
    mutation Identify($input: IdentifyMetadataInput!) {
        metadataIdentify(input: $input)
    }
    """
    variables = {
        "input": {
            "sceneIDs": scene_ids,
            "sources": [{"source": "STASHDB"}, {"source": "THEPORNDB"}],
            "options": {"setCoverImage": True, "setOrganized": False, "includeMalePerformers": True}
        }
    }
    result = graphql_request(stash_conn, query, variables)
    if result and 'data' in result:
        log_info(f"✅ Identify 已触发")
        return True
    else:
        log_error(f"⚠️ Identify 失败: {result}")
        return False

def get_recent_scenes(stash_conn, path, minutes=10):
    from datetime import datetime, timedelta
    min_time = (datetime.now() - timedelta(minutes=minutes)).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    query = """
    query FindRecentScenes($minTime: Timestamp!, $path: String!) {
        findScenes(
            filter: {per_page: 100}
            scene_filter: {
                path: {modifier: INCLUDES, value: $path}
                created_at: {modifier: GREATER_THAN, value: $minTime}
            }
        ) {
            scenes {
                id
                title
                path
                date
                rating100
                performers { name }
                studio { name }
                tags { name }
                files { width height duration video_codec audio_codec }
            }
        }
    }
    """
    result = graphql_request(stash_conn, query, {'minTime': min_time, 'path': path})
    if result and 'data' in result and result['data']:
        return result['data']['findScenes'].get('scenes', [])
    return []

def generate_nfo(scene, output_path):
    try:
        import xml.etree.ElementTree as ET
        root = ET.Element("movie")
        
        title = ET.SubElement(root, "title")
        title.text = scene.get('title', '') or ''
        
        if scene.get('date'):
            premiered = ET.SubElement(root, "premiered")
            premiered.text = scene['date']
            year = ET.SubElement(root, "year")
            year.text = scene['date'][:4]
        
        if scene.get('rating100'):
            rating = ET.SubElement(root, "rating")
            rating.text = str(scene['rating100'] / 20.0)
        
        for performer in scene.get('performers', []):
            actor = ET.SubElement(root, "actor")
            name = ET.SubElement(actor, "name")
            name.text = performer.get('name', '')
        
        if scene.get('studio'):
            studio = ET.SubElement(root, "studio")
            studio.text = scene['studio'].get('name', '')
        
        for tag in scene.get('tags', []):
            genre = ET.SubElement(root, "genre")
            genre.text = tag.get('name', '')
            tag_elem = ET.SubElement(root, "tag")
            tag_elem.text = tag.get('name', '')
        
        files = scene.get('files', [])
        if files:
            file_info = files[0]
            resolution = ET.SubElement(root, "resolution")
            resolution.text = f"{file_info.get('width', 0)}x{file_info.get('height', 0)}"
            codec = ET.SubElement(root, "codec")
            codec.text = file_info.get('video_codec', '')
        
        tree = ET.ElementTree(root)
        tree.write(output_path, encoding='utf-8', xml_declaration=True)
        log_info(f"✅ NFO 已生成: {output_path}")
        return True
    except Exception as e:
        log_error(f"❌ NFO 生成失败: {e}")
        return False

def process_nfo_generation(scenes):
    log_info(f"📝 开始生成 {len(scenes)} 个 NFO 文件...")
    success_count = 0
    for scene in scenes:
        scene_path = scene.get('path', '')
        if not scene_path:
            continue
        base_path = os.path.splitext(scene_path)[0]
        nfo_path = base_path + ".nfo"
        if os.path.exists(nfo_path):
            log_debug(f"⏭️ NFO 已存在，跳过: {nfo_path}")
            continue
        if generate_nfo(scene, nfo_path):
            success_count += 1
        time.sleep(0.5)
    log_info(f"✅ NFO 生成完成: {success_count}/{len(scenes)}")

class PollingMonitor:
    """CD2 网盘轮询监控器（文件系统事件不可靠时使用）"""
    def __init__(self, config):
        self.config = config
        self.known_files = {}  # path -> mtime
        self.running = False
    
    def scan_directory(self, path):
        """扫描目录获取当前所有视频文件"""
        files = {}
        try:
            for root, dirs, filenames in os.walk(path):
                # 排除目录
                dirs[:] = [d for d in dirs if d.lower() not in 
                          [e.lower() for e in self.config.get("exclude_dirs", [])]]
                
                for filename in filenames:
                    ext = Path(filename).suffix.lower()
                    if ext in self.config.get("video_extensions", []):
                        full_path = os.path.join(root, filename)
                        try:
                            stat = os.stat(full_path)
                            files[full_path] = stat.st_mtime
                        except:
                            pass
        except Exception as e:
            log_error(f"扫描目录失败 {path}: {e}")
        return files
    
    def check_changes(self, watch_path):
        """检查文件变化"""
        current_files = self.scan_directory(watch_path)
        changes = []
        
        # 检测新文件
        for path, mtime in current_files.items():
            if path not in self.known_files:
                changes.append(path)
                log_info(f"📥 轮询检测到新文件: {path}")
            elif self.known_files[path] != mtime:
                # 文件修改（可能是写入完成）
                pass
        
        self.known_files = current_files
        return changes
    
    def start(self, callback):
        self.running = True
        watch_paths = self.config.get("watch_paths", [])
        poll_interval = self.config.get("poll_interval", 30)
        
        # 初始扫描建立基线
        log_info("🔄 轮询模式：建立文件基线...")
        for path in watch_paths:
            if os.path.exists(path):
                self.known_files.update(self.scan_directory(path))
        log_info(f"📊 初始发现 {len(self.known_files)} 个文件")
        
        while self.running:
            for path in watch_paths:
                if not self.running:
                    break
                if os.path.exists(path):
                    changes = self.check_changes(path)
                    for changed_file in changes:
                        if should_process_file(changed_file, self.config):
                            callback(os.path.dirname(changed_file))
            
            # 轮询间隔
            for _ in range(poll_interval):
                if not self.running:
                    break
                time.sleep(1)
    
    def stop(self):
        self.running = False

def monitor_loop(stash_conn, config):
    """监控循环"""
    global running, pending_scans, processing_lock
    
    watch_paths = config.get("watch_paths", [])
    scan_delay = config.get("scan_delay", 30)
    identify_delay = config.get("identify_delay", 120)
    nfo_delay = config.get("nfo_delay", 180)
    use_polling = config.get("use_polling", True)
    
    if not watch_paths:
        log_warn("⚠️ 没有配置监控路径")
        return
    
    def on_file_event(dir_path):
        """文件变化回调"""
        pending_scans.add(dir_path)
    
    if use_polling:
        # CD2 网盘使用轮询模式
        log_info("🔄 使用轮询模式（适合 CD2 挂载）")
        poller = PollingMonitor(config)
        poll_thread = threading.Thread(target=poller.start, args=(on_file_event,))
        poll_thread.daemon = True
        poll_thread.start()
    else:
        # 本地文件使用 watchdog
        log_info("👁️ 使用事件监听模式（适合本地磁盘）")
        try:
            from watchdog.observers import Observer
            from watchdog.events import FileSystemEventHandler
        except ImportError:
            log_error("❌ 请先安装 watchdog: pip3 install watchdog")
            return
        
        class Handler(FileSystemEventHandler):
            def on_created(self, event):
                if event.is_directory:
                    return
                if should_process_file(event.src_path, config):
                    on_file_event(os.path.dirname(event.src_path))
            def on_moved(self, event):
                if event.is_directory:
                    return
                if should_process_file(event.dest_path, config):
                    on_file_event(os.path.dirname(event.dest_path))
        
        observer = Observer()
        handler = Handler()
        
        for path in watch_paths:
            if os.path.exists(path):
                observer.schedule(handler, path, recursive=True)
                log_info(f"✅ 开始监控: {path}")
            else:
                log_warn(f"⚠️ 路径不存在: {path}")
        
        observer.start()
    
    log_info("🚀 监控已启动")
    
    # 主处理循环
    while running:
        time.sleep(1)
        
        if processing_lock or not pending_scans:
            continue
        
        processing_lock = True
        to_process = list(pending_scans)
        pending_scans.clear()
        
        for path in to_process:
            if not running:
                break
            
            log_info(f"🎬 开始处理目录: {path}")
            
            # Scan
            log_info(f"⏳ 等待 {scan_delay} 秒后 Scan...")
            time.sleep(scan_delay)
            if not running:
                break
            if not scan_path(stash_conn, path):
                continue
            
            # Identify
            log_info(f"⏳ 等待 {identify_delay} 秒后 Identify...")
            time.sleep(identify_delay)
            if not running:
                break
            
            scenes = get_recent_scenes(stash_conn, path, minutes=15)
            if scenes:
                log_info(f"🎯 找到 {len(scenes)} 个新场景")
                scene_ids = [s['id'] for s in scenes]
                run_auto_identify(stash_conn, scene_ids)
            else:
                log_warn("⚠️ 未找到新场景")
            
            # NFO
            log_info(f"⏳ 等待 {nfo_delay} 秒后生成 NFO...")
            time.sleep(nfo_delay)
            if not running:
                break
            
            updated_scenes = get_recent_scenes(stash_conn, path, minutes=20)
            if updated_scenes:
                process_nfo_generation(updated_scenes)
            else:
                process_nfo_generation(scenes)
            
            log_info(f"✅ 目录处理完成: {path}")
        
        processing_lock = False
    
    # 清理
    if use_polling:
        poller.stop()
    else:
        observer.stop()
        observer.join()
    
    log_info("👋 监控已停止")

def start_monitor(stash_conn, config):
    global running, monitor_thread
    
    if running:
        log_warn("⚠️ 监控已在运行中")
        return False
    
    if not config.get("enabled", True):
        log_warn("⚠️ 插件已禁用")
        return False
    
    running = True
    monitor_thread = threading.Thread(target=monitor_loop, args=(stash_conn, config))
    monitor_thread.daemon = True
    monitor_thread.start()
    log_info("✅ Auto Scanner 已启动")
    return True

def stop_monitor():
    global running
    if not running:
        log_warn("⚠️ 监控未在运行")
        return False
    running = False
    log_info("⏳ 正在停止监控...")
    return True

def get_status():
    global running, processing_lock, pending_scans
    return {
        "running": running,
        "processing": processing_lock,
        "pending_count": len(pending_scans),
    }

def main():
    global running
    
    try:
        json_input = json.loads(sys.stdin.read())
    except json.JSONDecodeError:
        log_error("无法解析输入 JSON")
        sys.exit(1)
    
    stash_conn = json_input.get("server_connection", {})
    args = json_input.get("args", {})
    config = parse_settings(args)
    
    mode = args.get("mode", "auto")
    
    if mode == "start":
        success = start_monitor(stash_conn, config)
        output = {"status": "started" if success else "failed"}
        if not success and running:
            output["reason"] = "already_running"
        elif not success:
            output["reason"] = "disabled"
        print(json.dumps(output))
    
    elif mode == "stop":
        success = stop_monitor()
        print(json.dumps({"status": "stopped" if success else "not_running"}))
    
    elif mode == "status":
        status = get_status()
        print(json.dumps({
            "status": "running" if status["running"] else "stopped",
            "processing": status["processing"],
            "pending_count": status["pending_count"],
        }))
    
    elif mode == "reload":
        stop_monitor()
        time.sleep(1)
        success = start_monitor(stash_conn, config)
        print(json.dumps({"status": "reloaded" if success else "failed"}))
    
    elif mode == "auto":
        if config.get("auto_start", True) and config.get("enabled", True):
            start_monitor(stash_conn, config)
        print(json.dumps({"status": "ready"}))
    
    else:
        print(json.dumps({"error": f"未知模式: {mode}"}))

if __name__ == "__main__":
    main()
