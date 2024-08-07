﻿using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Papst.EventStore.CodeGeneration
{
  /// <summary>
  /// This Code Generator reads all classes during a compilation that are attributed with the
  /// EventNameAttribute defined in Papst.EventStore.Aggregation.EventRegistration
  /// Each Class or Record is then added to a registration which is passed to the DI
  /// </summary>
  [Generator]
  public class EventRegistrationCodeGenerator : ISourceGenerator
  {
    private const string EventAggregatorBaseClassName = "EventAggregatorBase";
    private const string EventAggregatorInterfaceName = "IEventAggregator";
    private const string ClassName = "EventStoreEventAggregator";

    public void Execute(GeneratorExecutionContext context)
    {
      var entryPoint = context.Compilation.GetEntryPoint(context.CancellationToken);
      string baseNamespace;
      if (entryPoint != null)
      {
        baseNamespace = entryPoint.ContainingNamespace.ToDisplayString();
      }
      else
      {
        baseNamespace = context.Compilation.Assembly.MetadataName;
      }

      if (string.IsNullOrEmpty(baseNamespace))
      {
        throw new NotSupportedException("Assembly Should have a Base Namespace - no Namespace found to be used");
      }

      // based on https://andrewlock.net/using-source-generators-with-a-custom-attribute--to-generate-a-nav-component-in-a-blazor-app/
      var allNodes = context.Compilation.SyntaxTrees.SelectMany(s => s.GetRoot().DescendantNodes());

      // find all class and record declarations
      var allClasses = allNodes.Where(d => d.IsKind(SyntaxKind.ClassDeclaration) || d.IsKind(SyntaxKind.RecordDeclaration)).OfType<TypeDeclarationSyntax>();

      // filter for all Classes and records that are attributed with EventNameAttribute
      var events = allClasses
        .Select(c => FindEvents(context.Compilation, c))
        .Where(c => c != null)
        .ToList();

      // filter for all Classes that are implementing either EventAggregatorBase<,> or IEventAggregator
      try
      {
        var aggregators = allClasses
          .OfType<ClassDeclarationSyntax>()
          .Where(c => c.DescendantNodes().OfType<SimpleBaseTypeSyntax>().Any(bt =>
            bt.Type is GenericNameSyntax &&
            (
              ((GenericNameSyntax)bt.Type).Identifier.ValueText == EventAggregatorBaseClassName ||
              ((GenericNameSyntax)bt.Type).Identifier.ValueText == EventAggregatorInterfaceName)
            )
          )
          .Select(c => new
          {
            Class = c.Identifier.ValueText,
            Namespace = FindNamespace(c),
            TypeArguments = c
              .DescendantNodes()
              .OfType<SimpleBaseTypeSyntax>()
              .Where(bt => ((GenericNameSyntax)bt.Type).Identifier.ValueText == EventAggregatorBaseClassName || ((GenericNameSyntax)bt.Type).Identifier.ValueText == EventAggregatorInterfaceName)
              .Select(bt => new
              {
                BT = bt,
                Entity = ((IdentifierNameSyntax)((GenericNameSyntax)bt.Type).TypeArgumentList.Arguments[0]).Identifier.ValueText,
                Event = ((IdentifierNameSyntax)((GenericNameSyntax)bt.Type).TypeArgumentList.Arguments[1]).Identifier.ValueText
              })
              .Select(bt => new
              {
                Entity = bt.Entity,
                EntityNamespace = FindNamespace(bt.Entity, allClasses),
                Event = bt.Event,
                EventNamespace = FindNamespace(bt.Event, allClasses)
              })
          })
          .ToList();

        if (events.Count > 0 || aggregators.Count > 0)
        {
          // Create a new Static class 
          StringBuilder builder = new StringBuilder();
          builder
            .AppendLine("/// <auto-generated>")
            .AppendLine("///  This code was generated Papst.EventStore.CodeGeneration")
            .AppendLine("///  See https://github.com/PapstIO/Papst.EventStore for more information")
            .AppendLine("/// </auto-generated>")
            .AppendLine("using Microsoft.Extensions.DependencyInjection;")
            .AppendLine("using System.Linq;")
            .AppendLine($"namespace {baseNamespace};")
            .AppendLine($"public static class {ClassName}")
            .AppendLine("{")
            .AppendLine("  public static IServiceCollection AddCodeGeneratedEvents(this IServiceCollection services)")
            .AppendLine("  {")
            ;

          // create a registration for each found event
          if (events.Count > 0)
          {
            builder.AppendLine("    var registration = new Papst.EventStore.EventRegistration.EventDescriptionEventRegistration();");
            foreach (var evt in events)
            {
              builder.AppendLine($"    registration.AddEvent<{evt.Value.NameSpace}.{evt.Value.Name}>({string.Join(", ", evt.Value.Attributes.Select(attr => $"new Papst.EventStore.EventRegistration.EventAttributeDescriptor(\"{attr.Name}\", {(attr.IsWrite ? bool.TrueString.ToLower() : bool.FalseString.ToLower())})"))});");
            }
            builder.AppendLine("    services.AddSingleton<Papst.EventStore.EventRegistration.IEventRegistration>(registration);");
          }

          // create a registration for each found aggregator
          if (aggregators.Count > 0)
          {
            foreach (var aggregator in aggregators)
            {
              foreach (var implementation in aggregator.TypeArguments)
              {
                builder.AppendLine($"    services.AddTransient<Papst.EventStore.Aggregation.IEventAggregator<{implementation.EntityNamespace}.{implementation.Entity}, {implementation.EventNamespace}.{implementation.Event}>, {aggregator.Namespace}.{aggregator.Class}>();");
              }
            }
          }

          // make sure only one instance of the IEventTypeProvider is registered
          builder
            .AppendLine("    if (!services.Any(descriptor => descriptor.ServiceType == typeof(Papst.EventStore.IEventTypeProvider)))")
            .AppendLine("    {")
            .AppendLine("      services.AddTransient<Papst.EventStore.IEventTypeProvider, Papst.EventStore.EventRegistration.EventRegistrationTypeProvider>();")
            .AppendLine("    }")
            ;

          builder.AppendLine("   return services;");

          builder
            .AppendLine("  }")
            .AppendLine("}");

          context.AddSource("EventRegistration.g.cs", builder.ToString());
        }
        else
        {
          context.ReportDiagnostic(Diagnostic.Create(new DiagnosticDescriptor(
            "EVTSRC0002",
            title: "No Events or Aggregators found to register",
            messageFormat: "No Events or Aggregators found to register",
            category: "EventRegistrationCodeGen",
            defaultSeverity: DiagnosticSeverity.Info,
            isEnabledByDefault: true,
            customTags: null
            ), Location.None));
        }
      }
      catch (Exception ex)
      {
        context.ReportDiagnostic(Diagnostic.Create(new DiagnosticDescriptor(
          "EVTSRC0001",
          title: "Failed to create Registrations",
          messageFormat: $"Failed to create Registrations: {ex.Message}",
          category: "EventRegistrationCodeGen",
          defaultSeverity: DiagnosticSeverity.Error,
          isEnabledByDefault: true,
          customTags: new[] { ex.Message }),
          Location.None));
        Console.WriteLine(ex.ToString());
      }
    }

    /// <summary>
    /// Checks if the <see cref="TypeDeclarationSyntax"/> is attributed with an EventNameAttribute
    /// and parses the values
    /// </summary>
    /// <param name="compilation"></param>
    /// <param name="typeDeclaration"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    private static (List<(string Name, bool IsWrite)> Attributes, string Name, string NameSpace)? FindEvents(Compilation compilation, TypeDeclarationSyntax typeDeclaration)
    {
      // get all Attributes by name of the Type
      var attributes = typeDeclaration.AttributeLists
        .SelectMany(x => x.Attributes)
        .Where(attr => attr.Name.ToString() == "EventNameAttribute" || attr.Name.ToString() == "EventName")
        .ToList();

      if (attributes.Count == 0)
      {
        return null;
      }

      var semanticModel = compilation.GetSemanticModel(typeDeclaration.SyntaxTree);
      var className = typeDeclaration.Identifier.ValueText;
      string nsName = FindNamespace(typeDeclaration);

      List<(string Name, bool IsWrite)> setAttributes = new List<(string Name, bool IsWrite)>();
      foreach (var attr in attributes)
      {
        string name = null;
        bool isWrite = true;
        var expr = semanticModel.GetConstantValue(attr.ArgumentList.Arguments[0].Expression).Value;

        // parse name and write flag
        if (expr is string name2)
        {
          name = name2;
        }
        else if (expr is bool isWrite2)
        {
          isWrite = isWrite2;
        }

        if (attr.ArgumentList.Arguments.Count > 1)
        {
          expr = semanticModel.GetConstantValue(attr.ArgumentList.Arguments[1].Expression).Value;
          if (expr is string name3)
          {
            name = name3;
          }
          else if (expr is bool isWrite2)
          {
            isWrite = isWrite2;
          }
        }
        if (name != null)
        {
          setAttributes.Add((name, isWrite));
        }
      }

      if (setAttributes.Count > 0)
      {
        return (setAttributes, className, nsName);
      }
      return null;
    }

    private static string FindNamespace(TypeDeclarationSyntax typeDeclaration)
    {
      if (typeDeclaration.Parent is BaseNamespaceDeclarationSyntax nsDeclBase)
      {
        if (nsDeclBase.Name is QualifiedNameSyntax qNameSyntax)
        {
          return nsDeclBase.Name.GetText().ToString();
        }
        else if (nsDeclBase.Name is SimpleNameSyntax nameSyntax)
        {
          return nameSyntax.Identifier.ValueText;
        }
        else
        {
          throw new ClassDeclarationNotFoundException();
        }
      }
      else if (typeDeclaration.Parent is FileScopedNamespaceDeclarationSyntax fsNsDecl)
      {
        return ((SimpleNameSyntax)fsNsDecl.Name).Identifier.ValueText;
      }
      else if (typeDeclaration.Parent is NamespaceDeclarationSyntax nsDecl)
      {
        return ((IdentifierNameSyntax)nsDecl.Name).Identifier.ValueText;
      }
      else
      {
        throw new InvalidOperationException($"Unable to find Namespace for Event Class {typeDeclaration.Identifier.ValueText}");
      }
    }

    /// <summary>
    /// Finds a ClassDeclaration in and parses the Namespace out of it
    /// </summary>
    /// <param name="className"></param>
    /// <param name="allClassDeclarations"></param>
    /// <returns></returns>
    /// <exception cref="ClassDeclarationNotFoundException"></exception>
    private static string FindNamespace(string className, IEnumerable<TypeDeclarationSyntax> allClassDeclarations)
    {
      // this may have issues with equal named classes!
      var classDeclaration = allClassDeclarations.Where(c => c.Identifier.ValueText == className).FirstOrDefault();
      if (classDeclaration == null)
      {
        throw new ClassDeclarationNotFoundException($"Unable to find ClassDeclaration of {className} for Code Generation");
      }

      return FindNamespace(classDeclaration);
    }

    public void Initialize(GeneratorInitializationContext context)
    {
      // none
      //System.Diagnostics.Debugger.Launch();
    }
  }
}
