import 'dart:async';
import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:http/http.dart' as http;

void main() {
  runApp(const TradingAlertsApp());
}

class TradingAlertsApp extends StatelessWidget {
  const TradingAlertsApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Сигналы',
      theme: ThemeData.dark(), // Темная тема отлично подходит для трейдинга
      home: const AlertsScreen(),
    );
  }
}

class AlertsScreen extends StatefulWidget {
  const AlertsScreen({super.key});

  @override
  State<AlertsScreen> createState() => _AlertsScreenState();
}

class _AlertsScreenState extends State<AlertsScreen> {
  // История всех пачек сообщений
  final List<List<Map<String, dynamic>>> _historicalBatches = [];

  // Текущий буфер, куда складываются сообщения из минутного спайка
  final List<Map<String, dynamic>> _currentBatch = [];

  // Таймер для отсчета 3 секунд "тишины"
  Timer? _debounceTimer;

  // Состояние подключения
  bool _isConnected = false;

  @override
  void initState() {
    super.initState();
    _connectToSSE();
  }

  void _connectToSSE() async {
    // ВАЖНО: Если запускаете на эмуляторе Android, вместо localhost нужно использовать 10.0.2.2
    // Если на реальном телефоне - укажите локальный IP вашего компьютера (например, 192.168.1.50)
    final url = Uri.parse('http://192.168.1.221:8000/stream');
    final client = http.Client();
    final request = http.Request('GET', url);

    try {
      final response = await client.send(request);
      setState(() => _isConnected = true);

      // Читаем поток по строкам
      response.stream
          .transform(utf8.decoder)
          .transform(const LineSplitter())
          .listen((line) {
        if (line.startsWith('data: ')) {
          final dataString = line.substring(6);
          if (dataString.isNotEmpty) {
            final Map<String, dynamic> alert = jsonDecode(dataString);
            _handleIncomingAlert(alert);
          }
        }
      }, onDone: () {
        setState(() => _isConnected = false);
      }, onError: (error) {
        setState(() => _isConnected = false);
      });
    } catch (e) {
      setState(() => _isConnected = false);
      print('Ошибка подключения: $e');
    }
  }

  void _handleIncomingAlert(Map<String, dynamic> alert) {
    // 1. Добавляем алерт в текущий невидимый буфер
    _currentBatch.add(alert);

    // 2. Сбрасываем предыдущий таймер, так как пришло новое сообщение
    _debounceTimer?.cancel();

    // 3. Заводим таймер заново. Если 3 секунды сообщений не будет - сработает коллбэк
    _debounceTimer = Timer(const Duration(seconds: 3), () {
      if (_currentBatch.isNotEmpty) {
        setState(() {
          // Копируем буфер в историю (добавляем в начало списка)
          _historicalBatches.insert(0, List.from(_currentBatch));
          // Очищаем буфер для следующей минуты
          _currentBatch.clear();
        });
      }
    });
  }

  @override
  void dispose() {
    _debounceTimer?.cancel();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Поток сигналов'),
        actions: [
          Icon(
            _isConnected ? Icons.wifi : Icons.wifi_off,
            color: _isConnected ? Colors.green : Colors.red,
          ),
          const SizedBox(width: 16),
        ],
      ),
      body: ListView.builder(
        padding: const EdgeInsets.all(8.0),
        itemCount: _historicalBatches.length,
        itemBuilder: (context, index) {
          final batch = _historicalBatches[index];
          return BatchCard(batch: batch);
        },
      ),
    );
  }
}

// Виджет для отрисовки одной пачки сообщений
class BatchCard extends StatelessWidget {
  final List<Map<String, dynamic>> batch;

  const BatchCard({super.key, required this.batch});

  @override
  Widget build(BuildContext context) {
    // Берем время из первого алерта в пачке для заголовка
    final String batchTime = batch.first['timestamp_utc'] ?? '';

    return Card(
      margin: const EdgeInsets.symmetric(vertical: 8.0),
      elevation: 4,
      child: Padding(
        padding: const EdgeInsets.all(12.0),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            // Заголовок пачки
            Container(
              padding: const EdgeInsets.only(bottom: 8.0),
              decoration: BoxDecoration(
                border: Border(bottom: BorderSide(color: Colors.grey.shade700)),
              ),
              child: Row(
                mainAxisAlignment: MainAxisAlignment.spaceBetween,
                children: [
                  Text(
                    '🔔 ${batch.length} алертов на текущую минуту',
                    style: const TextStyle(fontWeight: FontWeight.bold, fontSize: 16),
                  ),
                  Text(
                    batchTime.split('T').last.split('+').first, // Достаем только "12:35:00"
                    style: const TextStyle(color: Colors.grey, fontSize: 12),
                  ),
                ],
              ),
            ),
            const SizedBox(height: 8),
            // Список самих алертов внутри пачки
            ...batch.map((alert) => _buildAlertRow(alert)),
          ],
        ),
      ),
    );
  }

  Widget _buildAlertRow(Map<String, dynamic> alert) {
    final bool isUp = alert['direction'] == 'РОСТ';
    final Color directionColor = isUp ? Colors.greenAccent : Colors.redAccent;

    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 4.0),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.spaceBetween,
        children: [
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  alert['ticker'],
                  style: const TextStyle(fontWeight: FontWeight.bold),
                ),
                Text(
                  alert['name'],
                  style: const TextStyle(fontSize: 12, color: Colors.grey),
                  overflow: TextOverflow.ellipsis,
                ),
              ],
            ),
          ),
          Column(
            crossAxisAlignment: CrossAxisAlignment.end,
            children: [
              Text(
                '${alert['candle_close']}',
                style: const TextStyle(fontWeight: FontWeight.bold),
              ),
              Text(
                '${isUp ? '+' : ''}${alert['change_percent']}%',
                style: TextStyle(fontSize: 12, color: directionColor),
              ),
            ],
          )
        ],
      ),
    );
  }
}