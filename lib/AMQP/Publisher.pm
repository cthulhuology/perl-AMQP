package AMQP::Publisher;

use Mojo::Base -base;
use AnyEvent::RabbitMQ;
use Sys::Hostname;

has 'debug' => 1;
has 'host' => 'localhost';
has 'port' => 5672;
has 'user' => 'guest';
has 'password' => 'guest';
has 'vhost' => '/';
has 'timeout' => 1;
has 'heartbeat' => 30;
has 'exchange' => 'log';
has 'type' => 'topic';
has 'key' => '#';
has 'rabbit';
has 'connection';
has 'channel';
has 'status';
has 'on_message';

sub amqp {
	my ($self,$url) = @_;
	$url =~ /(?:amqp:\/\/)*(?<hostname>[^:\/]+):(?<port>\d+)\/(?<vhost>[^\/]*)/;
	$self->host($+{'host'} || 'localhost');
	$self->port($+{'port'} || 5672);
	$self->vhost($+{'vhost'} || '/');
	say "amqp://" . $self->host . ":" . $self->port . $self->vhost if $self->debug;
}

sub attach {
	my $self = shift;
	$self->status(AnyEvent->condvar);
	$self->rabbit(AnyEvent::RabbitMQ->new);
	$self->rabbit->load_xml_spec();
	$self->rabbit->connect(
		host => $self->host,
		port => $self->port,
		user => $self->user,
		pass => $self->password,
		vhost => $self->vhost,
		timeout => $self->timeout,
		tune => { heartbeat => $self->heartbeat },
		on_success => sub {
			say "Connected to amqp://" . $self->host . ":" . $self->port . $self->vhost if $self->debug;
			$self->connection(shift);
			$self->connection->open_channel(
				on_failure => $self->status,
				on_close => sub {
					say "Channel closed" if $self->debug;
					$self->status->send;
				},
				on_success => sub {
					say "Opened channel" if $self->debug;
					$self->channel(shift);
					$self->callback->($self);
				},
			);
		},
		on_failure => $self->status,
		on_read_failure =>  sub {
			say "Failed to read" if $self->debug;
			$self->status->send;
		},
		on_return => sub {
			say "Failed to send" if $self->debug;
			$self->status->send;
		},
		on_close => sub {
			say "Connection closed" if $self->debug;
			$self->status->send;
		}
	);
	$self->status->recv;
}

1;
