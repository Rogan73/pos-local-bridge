import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:flutter/material.dart';
import 'package:flutter_foreground_task/flutter_foreground_task.dart';

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  FlutterForegroundTask.initCommunicationPort();
  runApp(const MaterialApp(
    debugShowCheckedModeBanner: false,
    home: BridgeHomePage(),
  ));
}

@pragma('vm:entry-point')
void startCallback() {
  FlutterForegroundTask.setTaskHandler(BridgeTaskHandler());
}

class BridgeTaskHandler extends TaskHandler {
  HttpServer? _server;

  @override
  Future<void> onStart(DateTime timestamp, TaskStarter starter) async {
    await _startHttpServer();
  }

  @override
  void onRepeatEvent(DateTime timestamp) {}

  @override
  Future<void> onDestroy(DateTime timestamp) async {
    await _server?.close(force: true);
  }

  Future<void> _startHttpServer() async {
    try {
      _server = await HttpServer.bind(InternetAddress.loopbackIPv4, 8088, shared: true);
      _server!.listen(_handleRequest);
    } catch (e) {
      debugPrint('Server bind error: $e');
    }
  }

  void _setCorsHeaders(HttpResponse res) {
    res.headers.set('Access-Control-Allow-Origin', '*');
    res.headers.set('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
    res.headers.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    // Обов'язково для сучасних Chrome/Edge під час запиту з HTTPS на 127.0.0.1
    res.headers.set('Access-Control-Allow-Private-Network', 'true');
  }

  Future<void> _handleRequest(HttpRequest req) async {
    final res = req.response;
    _setCorsHeaders(res);

    // Обробка Preflight OPTIONS від браузера
    if (req.method == 'OPTIONS') {
      res.statusCode = HttpStatus.noContent;
      await res.close();
      return;
    }

    // Перевірка роботи мосту (GET http://127.0.0.1:8088/ping)
    if (req.method == 'GET' && req.uri.path == '/ping') {
      res.headers.contentType = ContentType.json;
      res.write(jsonEncode({'status': 'ok', 'service': 'POS Local Bridge'}));
      await res.close();
      return;
    }

    if (req.method == 'POST' && req.uri.path == '/send') {
      try {
        final bodyStr = await utf8.decoder.bind(req).join();
        final data = jsonDecode(bodyStr) as Map<String, dynamic>;

        final String host = data['host'];
        final int port = data['port'];
        final String mode = data['mode'] ?? 'privat_json';
        final int timeoutSec = data['timeout'] ?? 60;
        final String payload = data['payload'] ?? '';

        if (mode == 'raw_base64') {
          // Режим друку на IP-принтер (ESC/POS)
          final bytes = base64Decode(payload);
          final socket = await Socket.connect(host, port, timeout: Duration(seconds: timeoutSec));
          socket.add(bytes);
          await socket.flush();
          await socket.close();

          res.headers.contentType = ContentType.json;
          res.write(jsonEncode({'ok': true, 'mode': 'raw_base64'}));
        } else {
          // Режим JSON-протоколу ПриватБанку
          final terminalResponse = await _sendPrivatJson(host, port, payload, timeoutSec);
          res.headers.contentType = ContentType.json;
          res.write(terminalResponse);
        }
      } catch (e) {
        res.statusCode = HttpStatus.badGateway;
        res.headers.contentType = ContentType.json;
        res.write(jsonEncode({'ok': false, 'error': e.toString()}));
      } finally {
        await res.close();
      }
      return;
    }

    res.statusCode = HttpStatus.notFound;
    await res.close();
  }

  /// Робота з TCP-сокетом термінала ПриватБанку:
  /// Надсилає JSON і збирає вхідні JSON-пакети за балансом дужок {...},
  /// пропускаючи проміжні статусні повідомлення до отримання фінальної відповіді.
  Future<String> _sendPrivatJson(String host, int port, String jsonPayload, int timeoutSec) async {
    final socket = await Socket.connect(host, port, timeout: const Duration(seconds: 5));
    final completer = Completer<String>();
    final buffer = StringBuffer();

    final timer = Timer(Duration(seconds: timeoutSec), () {
      if (!completer.isCompleted) {
        socket.destroy();
        completer.completeError('Timeout waiting for POS terminal response ($timeoutSec s)');
      }
    });

    socket.listen(
      (List<int> chunk) {
        buffer.write(utf8.decode(chunk, allowMalformed: true));
        final packets = _extractCompleteJsonObjects(buffer);

        for (final pkt in packets) {
          try {
            final parsed = jsonDecode(pkt);
            if (parsed is Map<String, dynamic>) {
              // У протоколі ПриватБанку проміжні повідомлення містять step > 0 і не містять error/params із результатом
              // Фінальний пакет завжди містить ключ 'error' (true/false)
              if (parsed.containsKey('error')) {
                timer.cancel();
                socket.destroy();
                if (!completer.isCompleted) {
                  completer.complete(pkt);
                }
                return;
              }
            }
          } catch (_) {
            // Ігноруємо невалідний фрагмент, якщо він раптом потрапив
          }
        }
      },
      onError: (err) {
        timer.cancel();
        if (!completer.isCompleted) completer.completeError(err);
      },
      onDone: () {
        timer.cancel();
        if (!completer.isCompleted) {
          final rem = buffer.toString().trim();
          if (rem.isNotEmpty) {
            completer.complete(rem);
          } else {
            completer.completeError('Terminal closed connection without final JSON');
          }
        }
      },
    );

    // Надсилаємо команду на термінал (зазвичай закінчується \n або \0, термінал Привату приймає чистий UTF-8 JSON)
    socket.add(utf8.encode('$jsonPayload\n'));
    await socket.flush();

    return completer.future;
  }

  /// Витягує завершені JSON-об'єкти {...} із потокового буфера TCP
  List<String> _extractCompleteJsonObjects(StringBuffer buffer) {
    final text = buffer.toString();
    final results = <String>[];
    int depth = 0;
    int startIndex = -1;
    bool inString = false;
    bool escaped = false;
    int lastConsumedIndex = 0;

    for (int i = 0; i < text.length; i++) {
      final char = text[i];

      if (escaped) {
        escaped = false;
        continue;
      }
      if (char == r'\') {
        escaped = true;
        continue;
      }
      if (char == '"') {
        inString = !inString;
        continue;
      }
      if (inString) continue;

      if (char == '{') {
        if (depth == 0) startIndex = i;
        depth++;
      } else if (char == '}') {
        depth--;
        if (depth == 0 && startIndex != -1) {
          results.add(text.substring(startIndex, i + 1));
          lastConsumedIndex = i + 1;
          startIndex = -1;
        }
      }
    }

    if (lastConsumedIndex > 0) {
      final leftover = text.substring(lastConsumedIndex);
      buffer.clear();
      buffer.write(leftover);
    }

    return results;
  }
}

class BridgeHomePage extends StatefulWidget {
  const BridgeHomePage({super.key});

  @override
  State<BridgeHomePage> createState() => _BridgeHomePageState();
}

class _BridgeHomePageState extends State<BridgeHomePage> {
  bool _isRunning = false;

  @override
  void initState() {
    super.initState();
    _initForegroundTask();
    _startService();
  }

  void _initForegroundTask() {
    FlutterForegroundTask.init(
      androidNotificationOptions: AndroidNotificationOptions(
        channelId: 'pos_bridge_channel',
        channelName: 'POS Bridge Service',
        channelDescription: 'Фоновий сервер для зв\'язку з банківським терміналом і принтером',
        channelImportance: NotificationChannelImportance.LOW,
        priority: NotificationPriority.LOW,
      ),
      iosNotificationOptions: const IOSNotificationOptions(
        showNotification: true,
        playSound: false,
      ),
      foregroundTaskOptions: ForegroundTaskOptions(
        eventAction: ForegroundTaskEventAction.nothing(),
        autoRunOnBoot: true, // Автозапуск під час перезавантаження телефону!
        allowWakeLock: true,
        allowWifiLock: true,
      ),
    );
  }

  Future<void> _startService() async {
    if (Platform.isAndroid) {
      await FlutterForegroundTask.requestNotificationPermission();
      await FlutterForegroundTask.requestIgnoreBatteryOptimization();
    }

    if (await FlutterForegroundTask.isRunningService) {
      await FlutterForegroundTask.restartService();
    } else {
      await FlutterForegroundTask.startService(
        serviceId: 256,
        notificationTitle: 'POS-міст активний (127.0.0.1:8088)',
        notificationText: 'Готовий до роботи з терміналом і принтером',
        callback: startCallback,
      );
    }
    setState(() => _isRunning = true);
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: const Color(0xFF0F172A),
      body: Center(
        child: Padding(
          padding: const EdgeInsets.all(24.0),
          child: Column(
            mainAxisAlignment: MainAxisAlignment.center,
            children: [
              Icon(
                _isRunning ? Icons.check_circle_outline : Icons.sync,
                color: _isRunning ? Colors.greenAccent : Colors.amber,
                size: 72,
              ),
              const SizedBox(height: 16),
              Text(
                _isRunning ? 'POS-міст запущено' : 'Запуск сервера...',
                style: const TextStyle(color: Colors.white, fontSize: 22, fontWeight: FontWeight.bold),
              ),
              const SizedBox(height: 8),
              const Text(
                'Адреса: http://127.0.0.1:8088\nМожете згорнути це вікно та відкрити браузер.',
                textAlign: TextAlign.center,
                style: TextStyle(color: Colors.white70, fontSize: 15),
              ),
              const SizedBox(height: 24),
              ElevatedButton(
                onPressed: _startService,
                child: const Text('Перезапустити службу'),
              ),
            ],
          ),
        ),
      ),
    );
  }
}
